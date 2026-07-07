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

/// The default `BrokerNodeGroupInfo` MSK fills in when the caller supplies none,
/// already normalized (its `connectivityInfo` / `storageInfo` sub-objects and
/// AWS defaults present).
pub(crate) fn default_bngi() -> Value {
    normalize_bngi(None)
}

/// A synthesized customer-managed KMS key ARN, in the AWS `key/{uuid}` form, for
/// a cluster's `EncryptionAtRest.DataVolumeKMSKeyId` when the caller omits one
/// (real MSK always echoes a resolved key ARN, so `encryption_info` round-trips
/// and `MatchResourceAttrRegionalARN(..., "kms", "key/.+")` matches).
fn default_kms_key_arn(region: &str, account: &str) -> String {
    format!("arn:aws:kms:{region}:{account}:key/{}", Uuid::new_v4())
}

/// Fill an `EncryptionInfo` with the sub-objects + AWS defaults `DescribeCluster`
/// always echoes: an `EncryptionAtRest.DataVolumeKMSKeyId` (synthesized when the
/// caller omits it) and an `EncryptionInTransit` with `ClientBroker` defaulting
/// to `TLS` and `InCluster` to `true`. Whatever the caller supplied wins.
pub(crate) fn normalize_encryption_info(
    user: Option<&Value>,
    region: &str,
    account: &str,
) -> Value {
    let kms = user
        .and_then(|e| e.get("encryptionAtRest"))
        .and_then(|r| r.get("dataVolumeKMSKeyId"))
        .cloned()
        .unwrap_or_else(|| json!(default_kms_key_arn(region, account)));
    let transit = user.and_then(|e| e.get("encryptionInTransit"));
    let client_broker = transit
        .and_then(|t| t.get("clientBroker"))
        .cloned()
        .unwrap_or(json!("TLS"));
    let in_cluster = transit
        .and_then(|t| t.get("inCluster"))
        .cloned()
        .unwrap_or(json!(true));
    json!({
        "encryptionAtRest": { "dataVolumeKMSKeyId": kms },
        "encryptionInTransit": { "clientBroker": client_broker, "inCluster": in_cluster },
    })
}

/// Fill a provisioned cluster's `StorageInfo` with the `EbsStorageInfo` +
/// `VolumeSize` default (1000 GiB) MSK always echoes. `ProvisionedThroughput` is
/// preserved verbatim when the caller set it and left OFF otherwise -- real MSK
/// omits the block on a cluster that never enabled provisioned throughput (the
/// provider asserts `provisioned_throughput.# == 0`), and only surfaces it once
/// it has been configured.
pub(crate) fn normalize_storage_info(user: Option<&Value>) -> Value {
    let mut ebs = user
        .and_then(|s| s.get("ebsStorageInfo"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    ebs.entry("volumeSize").or_insert_with(|| json!(1000));
    json!({ "ebsStorageInfo": Value::Object(ebs) })
}

/// Fill a `ConnectivityInfo` with the `PublicAccess` (default `Type: DISABLED`)
/// and `VpcConnectivity` (client-auth sub-object with every scheme disabled)
/// MSK always echoes on a provisioned cluster, so `DescribeCluster` round-trips
/// the `connectivity_info` block even for a minimal create. Whatever the caller
/// supplied for either sub-object wins; the other is defaulted.
pub(crate) fn normalize_connectivity_info(user: Option<&Value>) -> Value {
    let public_access = user
        .and_then(|c| c.get("publicAccess"))
        .and_then(Value::as_object)
        .map(|pa| {
            let mut pa = pa.clone();
            pa.entry("type").or_insert_with(|| json!("DISABLED"));
            Value::Object(pa)
        })
        .unwrap_or_else(|| json!({ "type": "DISABLED" }));
    // A create request never carries VpcConnectivity (the provider requires all
    // schemes disabled at create and applies it via a follow-up UpdateConnectivity),
    // but DescribeCluster always echoes the all-disabled default; a later update
    // supplies the enabled shape, which wins here.
    let vpc = user
        .and_then(|c| c.get("vpcConnectivity"))
        .cloned()
        .unwrap_or_else(default_vpc_connectivity);
    json!({ "publicAccess": public_access, "vpcConnectivity": vpc })
}

/// Normalize a serverless cluster's `Serverless` block into the shape
/// `DescribeClusterV2` echoes: each `VpcConfig` gets a synthesized default
/// `securityGroupIds` when the caller omits one (real MSK resolves a default
/// security group, which the provider reads back as `vpc_config.0.
/// security_group_ids.# == 1`), and `clientAuthentication.sasl.iam.enabled`
/// defaults to `true` (IAM is the serverless auth scheme).
pub(crate) fn normalize_serverless(user: Option<&Value>) -> Value {
    let vpc_configs: Vec<Value> = user
        .and_then(|s| s.get("vpcConfigs"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|cfg| {
            let mut m = cfg.as_object().cloned().unwrap_or_default();
            m.entry("subnetIds").or_insert_with(|| json!([]));
            let has_sg = m
                .get("securityGroupIds")
                .and_then(Value::as_array)
                .is_some_and(|a| !a.is_empty());
            if !has_sg {
                m.insert(
                    "securityGroupIds".into(),
                    json!([format!("sg-{:016x}", Uuid::new_v4().as_u128() as u64)]),
                );
            }
            Value::Object(m)
        })
        .collect();
    let iam_enabled = user
        .and_then(|s| s.get("clientAuthentication"))
        .and_then(|c| c.get("sasl"))
        .and_then(|s| s.get("iam"))
        .and_then(|i| i.get("enabled"))
        .cloned()
        .unwrap_or(json!(true));
    json!({
        "vpcConfigs": vpc_configs,
        "clientAuthentication": { "sasl": { "iam": { "enabled": iam_enabled } } },
    })
}

/// Fill an `OpenMonitoring` block with both Prometheus exporters present
/// (`jmxExporter` / `nodeExporter`, each defaulting `enabledInBroker` to false),
/// so `DescribeCluster` echoes the full shape the provider flattens even when
/// the caller set only one exporter.
pub(crate) fn normalize_open_monitoring(user: &Value) -> Value {
    let prom = user.get("prometheus");
    let jmx = prom
        .and_then(|p| p.get("jmxExporter"))
        .and_then(|j| j.get("enabledInBroker"))
        .cloned()
        .unwrap_or(json!(false));
    let node = prom
        .and_then(|p| p.get("nodeExporter"))
        .and_then(|n| n.get("enabledInBroker"))
        .cloned()
        .unwrap_or(json!(false));
    json!({
        "prometheus": {
            "jmxExporter": { "enabledInBroker": jmx },
            "nodeExporter": { "enabledInBroker": node },
        }
    })
}

/// The all-schemes-disabled `VpcConnectivity` default MSK echoes.
fn default_vpc_connectivity() -> Value {
    json!({
        "clientAuthentication": {
            "sasl": { "iam": { "enabled": false }, "scram": { "enabled": false } },
            "tls": { "enabled": false },
        }
    })
}

/// Fill a provisioned cluster's `BrokerNodeGroupInfo` with the sub-objects +
/// AWS defaults `DescribeCluster` always echoes (`brokerAZDistribution`,
/// `connectivityInfo`, `storageInfo`, and placeholder subnets/security groups/
/// instance type when omitted), so both an explicit and a minimal create
/// round-trip faithfully. The caller's values always win.
pub(crate) fn normalize_bngi(user: Option<&Value>) -> Value {
    let mut bngi = user.and_then(Value::as_object).cloned().unwrap_or_default();
    bngi.entry("brokerAZDistribution")
        .or_insert_with(|| json!("DEFAULT"));
    bngi.entry("clientSubnets")
        .or_insert_with(|| json!(["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"]));
    bngi.entry("securityGroups")
        .or_insert_with(|| json!(["sg-0123456789abcdef0"]));
    bngi.entry("instanceType")
        .or_insert_with(|| json!("kafka.m5.large"));
    let storage = normalize_storage_info(bngi.get("storageInfo"));
    bngi.insert("storageInfo".into(), storage);
    let connectivity = normalize_connectivity_info(bngi.get("connectivityInfo"));
    bngi.insert("connectivityInfo".into(), connectivity);
    Value::Object(bngi)
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
            normalize_serverless(body.get("serverless")),
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
            normalize_bngi(provisioned_src.get("brokerNodeGroupInfo")),
        );
        // `currentBrokerSoftwareInfo` reflects the create's Kafka version plus any
        // associated configuration (arn + revision), which the provider surfaces
        // as `configuration_info`.
        let mut sw = Map::new();
        sw.insert("kafkaVersion".into(), json!(kv));
        if let Some(ci) = provisioned_src
            .get("configurationInfo")
            .and_then(Value::as_object)
        {
            if let Some(a) = ci.get("arn") {
                sw.insert("configurationArn".into(), a.clone());
            }
            if let Some(r) = ci.get("revision") {
                sw.insert("configurationRevision".into(), r.clone());
            }
        }
        cluster.insert("currentBrokerSoftwareInfo".into(), Value::Object(sw));
        // `clientAuthentication` / `loggingInfo` / `openMonitoring` are echoed
        // ONLY when the caller supplied them: MSK returns no `ClientAuthentication`
        // for an auth-less cluster (the provider asserts `client_authentication.# ==
        // 0`) and no logging/monitoring block for a cluster that configured none.
        if let Some(ca) = provisioned_src.get("clientAuthentication") {
            cluster.insert("clientAuthentication".into(), ca.clone());
        }
        if let Some(li) = provisioned_src.get("loggingInfo") {
            cluster.insert("loggingInfo".into(), li.clone());
        }
        if let Some(om) = provisioned_src.get("openMonitoring") {
            cluster.insert("openMonitoring".into(), normalize_open_monitoring(om));
        }
        cluster.insert(
            "enhancedMonitoring".into(),
            provisioned_src
                .get("enhancedMonitoring")
                .cloned()
                .unwrap_or(json!("DEFAULT")),
        );
        cluster.insert(
            "encryptionInfo".into(),
            normalize_encryption_info(provisioned_src.get("encryptionInfo"), region, account),
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
    // DescribeReplicator returns the *description* shapes, which differ from the
    // create request: each `KafkaCluster` gains a `kafkaClusterAlias` (its MSK
    // cluster name), and each `ReplicationInfo`'s `sourceKafkaClusterArn`/
    // `targetKafkaClusterArn` become `sourceKafkaClusterAlias`/
    // `targetKafkaClusterAlias`. The provider maps the aliases back to the two
    // cluster ARNs on read, so aliasing by cluster name keeps them consistent.
    let kafka_clusters = to_kafka_cluster_descriptions(body.get("kafkaClusters"));
    let replication_info_list = to_replication_info_descriptions(body.get("replicationInfoList"));
    let record = json!({
        "replicatorArn": arn,
        "replicatorName": name,
        "replicatorState": "CREATING",
        "replicatorResourceArn": arn,
        "creationTime": created,
        "currentVersion": gen_version(n),
        "isReplicatorReference": false,
        "serviceExecutionRoleArn": body.get("serviceExecutionRoleArn").cloned().unwrap_or(json!("")),
        "kafkaClusters": kafka_clusters,
        "replicationInfoList": replication_info_list,
        "replicatorDescription": body.get("description").cloned().unwrap_or(json!("")),
    });
    data.replicators.insert(arn.clone(), record);
    insert_tags(data, &arn, body);
    Ok(arn)
}

/// The MSK-cluster alias the description shapes key on: the cluster's name.
pub(crate) fn cluster_alias(msk_cluster_arn: &str) -> String {
    shared::cluster_name_from_arn(msk_cluster_arn)
        .unwrap_or(msk_cluster_arn)
        .to_string()
}

/// Transform a `CreateReplicator` `kafkaClusters` list (request shape) into the
/// `KafkaClusterDescription` list `DescribeReplicator` returns, adding each
/// cluster's `kafkaClusterAlias`.
fn to_kafka_cluster_descriptions(clusters: Option<&Value>) -> Value {
    let out: Vec<Value> = clusters
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|c| {
            let mut m = c.as_object().cloned().unwrap_or_default();
            let msk_arn = m
                .get("amazonMskCluster")
                .and_then(|a| a.get("mskClusterArn"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            m.insert("kafkaClusterAlias".into(), json!(cluster_alias(msk_arn)));
            Value::Object(m)
        })
        .collect();
    Value::Array(out)
}

/// Transform a `CreateReplicator` `replicationInfoList` (request shape) into the
/// `ReplicationInfoDescription` list `DescribeReplicator` returns: replace the
/// source/target cluster ARNs with their aliases (the provider resolves the
/// ARNs back from the `kafkaClusters` alias map on read).
pub(crate) fn to_replication_info_descriptions(list: Option<&Value>) -> Value {
    let out: Vec<Value> = list
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|info| {
            let mut m = info.as_object().cloned().unwrap_or_default();
            if let Some(src) = m
                .remove("sourceKafkaClusterArn")
                .as_ref()
                .and_then(Value::as_str)
            {
                m.insert("sourceKafkaClusterAlias".into(), json!(cluster_alias(src)));
            }
            if let Some(tgt) = m
                .remove("targetKafkaClusterArn")
                .as_ref()
                .and_then(Value::as_str)
            {
                m.insert("targetKafkaClusterAlias".into(), json!(cluster_alias(tgt)));
            }
            // MSK fills computed defaults on the topic-replication policy: a
            // `startingPosition` (default `LATEST`) and `topicNameConfiguration`
            // (default `PREFIXED_WITH_SOURCE_CLUSTER_ALIAS`), which the provider
            // reads back as Computed blocks. Preserve whatever the caller set.
            if let Some(tr) = m
                .entry("topicReplication".to_string())
                .or_insert_with(|| json!({}))
                .as_object_mut()
            {
                tr.entry("startingPosition")
                    .or_insert_with(|| json!({ "type": "LATEST" }));
                tr.entry("topicNameConfiguration")
                    .or_insert_with(|| json!({ "type": "PREFIXED_WITH_SOURCE_CLUSTER_ALIAS" }));
            }
            Value::Object(m)
        })
        .collect();
    Value::Array(out)
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
    let target = body.get("targetClusterArn").cloned().unwrap_or(json!(""));
    let target_arn = target.as_str().unwrap_or_default();
    let target_account = shared::arn_account(target_arn).unwrap_or(account);
    let target_name = shared::cluster_name_from_arn(target_arn).unwrap_or("cluster");
    let arn = shared::vpc_connection_arn(region, account, target_account, target_name, &uuid, n);
    let created = now_iso();
    let subnets = body.get("clientSubnets").cloned().unwrap_or(json!([]));
    let sgs = body.get("securityGroups").cloned().unwrap_or(json!([]));
    let auth = body.get("authentication").cloned().unwrap_or(json!(""));
    let vpc_id = body.get("vpcId").cloned().unwrap_or(json!(""));
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
