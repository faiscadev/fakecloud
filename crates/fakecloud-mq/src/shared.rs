//! Primitives shared between the `mq` service handlers and the CloudFormation
//! provisioner so the two broker/configuration create paths cannot diverge
//! (the #1766 lesson: one create implementation, reused, not two parallel ones).
//!
//! The `create_broker_record` builder operates directly on a `&mut MqData` and
//! produces exactly the wire object the direct `CreateBroker` handler persists,
//! so a CFN-created broker reads back byte-for-byte identically on
//! `DescribeBroker`.

use base64::Engine as _;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::state::{BrokerDataPlane, MqData};

/// Whether an engine string names RabbitMQ, case-insensitively. Amazon MQ
/// echoes back the engine type in the display case the caller sent
/// (`ActiveMQ` / `RabbitMQ`), so all internal branching is case-insensitive.
pub fn is_rabbit(engine: &str) -> bool {
    engine.eq_ignore_ascii_case("RABBITMQ")
}

/// The engine's human display name, used in auto-generated config descriptions.
pub fn engine_display(engine: &str) -> &'static str {
    if is_rabbit(engine) {
        "RabbitMQ"
    } else {
        "ActiveMQ"
    }
}

pub fn default_engine_version(engine: &str) -> &'static str {
    if is_rabbit(engine) {
        "3.13"
    } else {
        "5.18"
    }
}

/// The default configuration `Data` for an engine, base64-encoded (matching
/// AWS's auto-generated broker configuration).
pub fn default_config_data(engine: &str) -> String {
    let xml = if is_rabbit(engine) {
        "consumer_timeout = 1800000\n".to_string()
    } else {
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\
         <broker xmlns=\"http://activemq.apache.org/schema/core\" start=\"false\">\n\
         </broker>\n"
            .to_string()
    };
    base64::engine::general_purpose::STANDARD.encode(xml.as_bytes())
}

/// FNV-1a hash for deterministic synthesis of ids / IPs.
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

pub fn broker_arn(region: &str, account: &str, name: &str, id: &str) -> String {
    format!("arn:aws:mq:{region}:{account}:broker:{name}:{id}")
}

pub fn config_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:mq:{region}:{account}:configuration:{id}")
}

/// The region embedded in a broker/configuration ARN (the 4th colon-delimited
/// field, `arn:aws:mq:<region>:...`). Used to scope name-uniqueness and
/// `List*` results to the request's region without a schema change.
pub fn arn_region(arn: &str) -> Option<&str> {
    arn.split(':').nth(3)
}

/// A broker's region, parsed from its stored `brokerArn`.
pub fn broker_region(broker: &Value) -> Option<&str> {
    broker
        .get("brokerArn")
        .and_then(Value::as_str)
        .and_then(arn_region)
}

/// A configuration's region, parsed from its stored `arn`.
pub fn config_region(cfg: &Value) -> Option<&str> {
    cfg.get("arn").and_then(Value::as_str).and_then(arn_region)
}

/// Deterministic default-VPC subnet ids AWS synthesizes when the caller omits
/// `SubnetIds`: one for `SINGLE_INSTANCE`, two for the multi-AZ deployment
/// modes. Shared so the direct API and the CFN provisioner produce identical
/// ids for a given broker id.
pub fn synthesize_subnets(id: &str, deployment_mode: &str) -> Vec<Value> {
    let n = if deployment_mode == "SINGLE_INSTANCE" {
        1
    } else {
        2
    };
    (0..n)
        .map(|i| {
            let h = hash_str(&format!("{id}-subnet-{i}"));
            json!(format!("subnet-{h:017x}"))
        })
        .collect()
}

/// Per-protocol wire endpoint lists for a broker, derived deterministically
/// from its id/engine/region/deployment mode. Both the service's
/// `brokerInstances` describe view and the CFN `Fn::GetAtt` list attributes are
/// projected from this single source.
#[derive(Default)]
pub struct BrokerEndpoints {
    pub ips: Vec<String>,
    pub console_urls: Vec<String>,
    pub open_wire: Vec<String>,
    pub amqp: Vec<String>,
    pub stomp: Vec<String>,
    pub mqtt: Vec<String>,
    pub wss: Vec<String>,
}

pub fn broker_endpoints(
    id: &str,
    engine: &str,
    region: &str,
    deployment_mode: &str,
    data_plane: Option<&BrokerDataPlane>,
) -> BrokerEndpoints {
    // When a real backing container is bound, project its REAL reachable host
    // + mapped ports so a client actually connects (the RDS `Address`
    // pattern). The response SHAPE is identical to the cosmetic path -- only
    // the host/port VALUES become real -- so conformance (which never spawns a
    // container) still passes.
    if let Some(dp) = data_plane {
        return real_broker_endpoints(engine, dp);
    }
    let count = if !is_rabbit(engine) && deployment_mode == "ACTIVE_STANDBY_MULTI_AZ" {
        2
    } else {
        1
    };
    let mut e = BrokerEndpoints::default();
    for i in 1..=count {
        let h = hash_str(&format!("{id}-{i}"));
        e.ips.push(format!(
            "10.{}.{}.{}",
            (h >> 16) & 0xff,
            (h >> 8) & 0xff,
            h & 0xff
        ));
        if is_rabbit(engine) {
            let host = format!("{id}.mq.{region}.amazonaws.com");
            e.console_urls.push(format!("https://{host}"));
            e.amqp.push(format!("amqps://{host}:5671"));
        } else {
            let host = format!("{id}-{i}.mq.{region}.amazonaws.com");
            e.console_urls.push(format!("https://{host}:8162"));
            e.open_wire.push(format!("ssl://{host}:61617"));
            e.amqp.push(format!("amqp+ssl://{host}:5671"));
            e.stomp.push(format!("stomp+ssl://{host}:61614"));
            e.mqtt.push(format!("mqtt+ssl://{host}:8883"));
            e.wss.push(format!("wss://{host}:61619"));
        }
    }
    e
}

/// Endpoints projected from a live backing container's real host + mapped
/// ports. The URLs use the plaintext scheme forms the real broker actually
/// speaks on the mapped ports (`tcp://`/`amqp://`/`stomp://`/`mqtt://`/`ws://`)
/// so a client can connect verbatim -- unlike the cosmetic `ssl://…amazonaws`
/// forms, these point at a socket that is genuinely listening. The IP is the
/// reachable host (`127.0.0.1` or the sibling alias), matching what the ports
/// are published on.
fn real_broker_endpoints(engine: &str, dp: &BrokerDataPlane) -> BrokerEndpoints {
    let host = dp.host.as_str();
    let port = |label: &str| dp.ports.get(label).copied();
    let mut e = BrokerEndpoints::default();
    e.ips.push(host.to_string());
    if is_rabbit(engine) {
        if let Some(p) = port("console") {
            e.console_urls.push(format!("https://{host}:{p}"));
        }
        if let Some(p) = port("amqp") {
            e.amqp.push(format!("amqp://{host}:{p}"));
        }
    } else {
        if let Some(p) = port("console") {
            e.console_urls.push(format!("http://{host}:{p}"));
        }
        if let Some(p) = port("openwire") {
            e.open_wire.push(format!("tcp://{host}:{p}"));
        }
        if let Some(p) = port("amqp") {
            e.amqp.push(format!("amqp://{host}:{p}"));
        }
        if let Some(p) = port("stomp") {
            e.stomp.push(format!("stomp://{host}:{p}"));
        }
        if let Some(p) = port("mqtt") {
            e.mqtt.push(format!("mqtt://{host}:{p}"));
        }
        if let Some(p) = port("ws") {
            e.wss.push(format!("ws://{host}:{p}"));
        }
    }
    e
}

/// Normalize an inline create-broker user (`users[]` member) into the stored
/// user object.
pub fn normalize_user(u: &Value) -> Value {
    json!({
        "username": u.get("username").cloned().unwrap_or(json!("")),
        "password": u.get("password").cloned().unwrap_or(json!("")),
        "consoleAccess": u.get("consoleAccess").cloned().unwrap_or(json!(false)),
        "groups": u.get("groups").cloned().unwrap_or(json!([])),
        "replicationUser": u.get("replicationUser").cloned().unwrap_or(json!(false)),
    })
}

/// Convert a request body's `tags` object member into a `BTreeMap`.
pub fn tags_from_body(b: &Value) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(obj) = b.get("tags").and_then(Value::as_object) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// Apply a configuration association to a broker's `configurations`, preserving
/// history: the prior `current` (if any) is pushed onto `history` and the new
/// `{id, revision}` becomes `current`. Shared by the service's pending-config
/// settle path semantics and the CFN `ConfigurationAssociation` / broker-update
/// paths so none of them wipe history.
pub fn set_broker_configuration(broker: &mut Map<String, Value>, config_id: &str, revision: i64) {
    let configs = broker
        .entry("configurations")
        .or_insert_with(|| json!({ "history": [] }));
    if let Some(ce) = configs.as_object_mut() {
        if let Some(old_current) = ce.get("current").cloned() {
            let hist = ce.entry("history").or_insert_with(|| json!([]));
            if let Some(arr) = hist.as_array_mut() {
                arr.push(old_current);
            }
        }
        ce.insert(
            "current".into(),
            json!({ "id": config_id, "revision": revision }),
        );
    }
}

/// Build and insert a broker record (plus its auto-generated ActiveMQ
/// configuration, inline users, and ARN-keyed tags) into `data`, returning
/// `(broker_id, broker_arn)`. The broker is created `CREATION_IN_PROGRESS`; the
/// caller settles it via the normal lifecycle (`reconcile_brokers`).
///
/// `b` is a restJson1 `CreateBroker` body (camelCase members). This is the one
/// broker-construction implementation; the direct handler wraps it with
/// idempotency + name-uniqueness + the wire response, and the CFN provisioner
/// translates its PascalCase properties into this body shape and reuses it.
pub fn create_broker_record(
    data: &mut MqData,
    account: &str,
    region: &str,
    b: &Value,
) -> (String, String) {
    let name = b
        .get("brokerName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let engine = b
        .get("engineType")
        .and_then(Value::as_str)
        .unwrap_or("ACTIVEMQ")
        .to_string();
    let deployment = b
        .get("deploymentMode")
        .and_then(Value::as_str)
        .unwrap_or("SINGLE_INSTANCE")
        .to_string();

    let id = format!("b-{}", Uuid::new_v4());
    let arn = broker_arn(region, account, &name, &id);
    let engine_version = b
        .get("engineVersion")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| default_engine_version(&engine).to_string());
    // AWS returns the authentication strategy lower-cased (`simple`); the
    // default when the caller omits it is `simple`.
    let auth = b
        .get("authenticationStrategy")
        .and_then(Value::as_str)
        .unwrap_or("simple")
        .to_string();

    let mut broker = Map::new();
    broker.insert("brokerId".into(), json!(id));
    broker.insert("brokerArn".into(), json!(arn));
    broker.insert("brokerName".into(), json!(name));
    broker.insert("brokerState".into(), json!("CREATION_IN_PROGRESS"));
    broker.insert("engineType".into(), json!(engine));
    broker.insert("engineVersion".into(), json!(engine_version));
    broker.insert("authenticationStrategy".into(), json!(auth));
    broker.insert("created".into(), json!(now_iso()));
    broker.insert("deploymentMode".into(), json!(deployment));
    broker.insert(
        "hostInstanceType".into(),
        b.get("hostInstanceType")
            .cloned()
            .unwrap_or(json!("mq.m5.large")),
    );
    broker.insert(
        "publiclyAccessible".into(),
        b.get("publiclyAccessible").cloned().unwrap_or(json!(false)),
    );
    broker.insert(
        "autoMinorVersionUpgrade".into(),
        b.get("autoMinorVersionUpgrade")
            .cloned()
            .unwrap_or(json!(false)),
    );
    // AWS returns the storage type lower-cased (`ebs` / `efs`).
    broker.insert(
        "storageType".into(),
        b.get("storageType").cloned().unwrap_or(json!("ebs")),
    );
    broker.insert(
        "securityGroups".into(),
        b.get("securityGroups").cloned().unwrap_or(json!([])),
    );
    // When no subnets are supplied, AWS places the broker in subnets of the
    // account's default VPC. Synthesize deterministic ids so DescribeBroker
    // returns a stable, non-empty list (Terraform's `subnet_ids` is Computed
    // and must round-trip through import).
    let subnets = match b.get("subnetIds").and_then(Value::as_array) {
        Some(a) if !a.is_empty() => Value::Array(a.clone()),
        _ => Value::Array(synthesize_subnets(&id, &deployment)),
    };
    broker.insert("subnetIds".into(), subnets);
    if let Some(m) = b.get("maintenanceWindowStartTime") {
        broker.insert("maintenanceWindowStartTime".into(), m.clone());
    } else {
        broker.insert(
            "maintenanceWindowStartTime".into(),
            json!({ "dayOfWeek": "SUNDAY", "timeOfDay": "00:00", "timeZone": "UTC" }),
        );
    }
    // EncryptionOptions is always present in DescribeBroker; the default is an
    // AWS-owned key.
    broker.insert(
        "encryptionOptions".into(),
        b.get("encryptionOptions")
            .cloned()
            .unwrap_or_else(|| json!({ "useAwsOwnedKey": true })),
    );
    let audit = b
        .get("logs")
        .and_then(|l| l.get("audit"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let general = b
        .get("logs")
        .and_then(|l| l.get("general"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut logs = json!({
        "audit": audit,
        "general": general,
        "generalLogGroup": format!("/aws/amazonmq/broker/{id}/general"),
    });
    if audit {
        logs["auditLogGroup"] = json!(format!("/aws/amazonmq/broker/{id}/audit"));
    }
    broker.insert("logs".into(), logs);
    broker.insert(
        "dataReplicationMode".into(),
        b.get("dataReplicationMode")
            .cloned()
            .unwrap_or(json!("NONE")),
    );

    // Users supplied inline at create time become the broker's current users.
    let mut user_map = std::collections::BTreeMap::new();
    if let Some(arr) = b.get("users").and_then(Value::as_array) {
        for u in arr {
            if let Some(username) = u.get("username").and_then(Value::as_str) {
                user_map.insert(username.to_string(), normalize_user(u));
            }
        }
    }

    // ActiveMQ brokers are backed by a configuration; RabbitMQ has none.
    if !is_rabbit(&engine) {
        let cfg = if let Some(c) = b.get("configuration") {
            json!({ "id": c.get("id").cloned().unwrap_or(json!("")), "revision": c.get("revision").cloned().unwrap_or(json!(1)) })
        } else {
            let cid = format!("c-{}", Uuid::new_v4());
            let created = now_iso();
            // latestRevision and the revision list must agree on the engine.
            let desc = format!("Auto-generated default for {}", engine_display(&engine));
            let rev = json!({ "revision": 1, "created": created, "description": desc });
            data.configurations.insert(
                cid.clone(),
                json!({
                    "arn": config_arn(region, account, &cid),
                    "authenticationStrategy": auth,
                    "created": created,
                    "description": desc,
                    "engineType": engine,
                    "engineVersion": engine_version,
                    "id": cid,
                    "latestRevision": rev,
                    "name": format!("{name}-configuration"),
                }),
            );
            data.configuration_revisions.insert(
                cid.clone(),
                vec![json!({
                    "revision": 1,
                    "created": created,
                    "description": desc,
                    "data": default_config_data(&engine),
                })],
            );
            json!({ "id": cid, "revision": 1 })
        };
        broker.insert(
            "configurations".into(),
            json!({ "current": cfg, "history": [] }),
        );
    }

    data.brokers.insert(id.clone(), Value::Object(broker));
    data.users.insert(id.clone(), user_map);

    let tags = tags_from_body(b);
    if !tags.is_empty() {
        data.tags.insert(arn.clone(), tags);
    }

    (id, arn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::BrokerDataPlane;
    use std::collections::BTreeMap;

    #[test]
    fn cosmetic_endpoints_used_without_a_data_plane() {
        // No backing container -> the well-formed synthetic endpoints (identical
        // response shape, so conformance is unaffected).
        let e = broker_endpoints("b-1", "ACTIVEMQ", "us-east-1", "SINGLE_INSTANCE", None);
        assert_eq!(e.open_wire.len(), 1);
        assert!(e.open_wire[0].contains("amazonaws.com"));
        assert!(e.open_wire[0].starts_with("ssl://"));
    }

    #[test]
    fn real_endpoints_projected_from_data_plane() {
        // A live container's real host + mapped ports are projected verbatim, so
        // a client connects to a genuinely listening socket.
        let mut ports = BTreeMap::new();
        ports.insert("openwire".to_string(), 51616u16);
        ports.insert("stomp".to_string(), 51613u16);
        ports.insert("console".to_string(), 58161u16);
        let dp = BrokerDataPlane {
            container_id: "c1".to_string(),
            host: "127.0.0.1".to_string(),
            ports,
        };
        let e = broker_endpoints("b-1", "ACTIVEMQ", "us-east-1", "SINGLE_INSTANCE", Some(&dp));
        assert_eq!(e.open_wire, vec!["tcp://127.0.0.1:51616".to_string()]);
        assert_eq!(e.stomp, vec!["stomp://127.0.0.1:51613".to_string()]);
        assert_eq!(e.console_urls, vec!["http://127.0.0.1:58161".to_string()]);
        assert_eq!(e.ips, vec!["127.0.0.1".to_string()]);
        // No amazonaws host anywhere.
        assert!(!e.open_wire[0].contains("amazonaws.com"));
    }

    #[test]
    fn real_rabbitmq_endpoints_use_amqp_scheme() {
        let mut ports = BTreeMap::new();
        ports.insert("amqp".to_string(), 55672u16);
        ports.insert("console".to_string(), 55673u16);
        let dp = BrokerDataPlane {
            container_id: "c1".to_string(),
            host: "host.docker.internal".to_string(),
            ports,
        };
        let e = broker_endpoints("b-1", "RABBITMQ", "us-east-1", "SINGLE_INSTANCE", Some(&dp));
        assert_eq!(
            e.amqp,
            vec!["amqp://host.docker.internal:55672".to_string()]
        );
        assert!(e.open_wire.is_empty());
    }
}
