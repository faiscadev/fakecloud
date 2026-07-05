//! `AWS::AmazonMQ::Broker`, `AWS::AmazonMQ::Configuration`, and
//! `AWS::AmazonMQ::ConfigurationAssociation` CloudFormation provisioning. Each
//! resource is written through to the `mq` service state as the same wire JSON
//! the direct `CreateBroker` / `CreateConfiguration` handlers store, so a
//! CFN-created broker or configuration reads back identically on
//! `DescribeBroker` / `DescribeConfiguration` and persists through the `mq`
//! snapshot hook (survives a restart -- the #1766 phantom-resource lesson).
//!
//! Physical id + `Ref`:
//!   Broker        -> `BrokerId` (`b-<uuid>`)
//!   Configuration -> `Id` (`c-<uuid>`)
//!
//! `Fn::GetAtt` (verified against the AWS resource spec):
//!   Broker        -> Arn, IpAddresses, OpenWireEndpoints, AmqpEndpoints,
//!                    StompEndpoints, MqttEndpoints, WssEndpoints,
//!                    ConfigurationId, ConfigurationRevision
//!   Configuration -> Arn, Id, Revision

use base64::Engine as _;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

impl ResourceProvisioner {
    // -------------------------------------------------------------- Broker

    pub(super) fn create_mq_broker(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = mq_str(props, "BrokerName").unwrap_or_else(|| resource.logical_id.clone());
        let engine = mq_str(props, "EngineType")
            .unwrap_or_else(|| "ACTIVEMQ".to_string())
            .to_uppercase();
        let deployment =
            mq_str(props, "DeploymentMode").unwrap_or_else(|| "SINGLE_INSTANCE".to_string());
        let region = self.region.clone();
        let account = self.account_id.clone();
        let id = format!("b-{}", Uuid::new_v4());
        let arn = format!("arn:aws:mq:{region}:{account}:broker:{name}:{id}");
        let engine_version = mq_str(props, "EngineVersion")
            .unwrap_or_else(|| default_engine_version(&engine).to_string());
        let auth = mq_str(props, "AuthenticationStrategy").unwrap_or_else(|| "SIMPLE".to_string());

        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if acct
            .brokers
            .values()
            .any(|b| b.get("brokerName").and_then(Value::as_str) == Some(name.as_str()))
        {
            return Err(format!("Broker name {name} already exists"));
        }

        let mut broker = Map::new();
        broker.insert("brokerId".into(), json!(id));
        broker.insert("brokerArn".into(), json!(arn));
        broker.insert("brokerName".into(), json!(name));
        // CloudFormation waits for the broker to be RUNNING before CREATE_COMPLETE,
        // so provision it already running.
        broker.insert("brokerState".into(), json!("RUNNING"));
        broker.insert("engineType".into(), json!(engine));
        broker.insert("engineVersion".into(), json!(engine_version));
        broker.insert("authenticationStrategy".into(), json!(auth));
        broker.insert("created".into(), json!(now_iso()));
        broker.insert("deploymentMode".into(), json!(deployment.clone()));
        broker.insert(
            "hostInstanceType".into(),
            json!(mq_str(props, "HostInstanceType").unwrap_or_else(|| "mq.m5.large".to_string())),
        );
        broker.insert(
            "publiclyAccessible".into(),
            json!(mq_bool(props, "PubliclyAccessible")),
        );
        broker.insert(
            "autoMinorVersionUpgrade".into(),
            json!(mq_bool(props, "AutoMinorVersionUpgrade")),
        );
        broker.insert(
            "storageType".into(),
            json!(mq_str(props, "StorageType").unwrap_or_else(|| "EBS".to_string())),
        );
        broker.insert(
            "securityGroups".into(),
            mq_string_list(props, "SecurityGroups"),
        );
        broker.insert("subnetIds".into(), mq_string_list(props, "SubnetIds"));
        broker.insert(
            "logs".into(),
            json!({
                "audit": false,
                "general": false,
                "generalLogGroup": format!("/aws/amazonmq/broker/{id}/general"),
            }),
        );

        // A referenced configuration becomes the broker's current configuration.
        let (config_id, config_rev) = if let Some(cfg) = props.get("Configuration") {
            let cid = cfg
                .get("Id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let rev = cfg.get("Revision").and_then(Value::as_i64).unwrap_or(1);
            broker.insert(
                "configurations".into(),
                json!({ "current": { "id": cid, "revision": rev }, "history": [] }),
            );
            (cid, rev)
        } else if engine == "ACTIVEMQ" {
            let cid = format!("c-{}", Uuid::new_v4());
            let created = now_iso();
            acct.configurations.insert(
                cid.clone(),
                json!({
                    "arn": format!("arn:aws:mq:{region}:{account}:configuration:{cid}"),
                    "authenticationStrategy": auth,
                    "created": created,
                    "description": "",
                    "engineType": engine,
                    "engineVersion": engine_version,
                    "id": cid,
                    "latestRevision": { "revision": 1, "created": created, "description": "Auto-generated default for ActiveMQ" },
                    "name": format!("{name}-configuration"),
                }),
            );
            acct.configuration_revisions.insert(
                cid.clone(),
                vec![json!({
                    "revision": 1,
                    "created": created,
                    "description": "Auto-generated default for ActiveMQ",
                    "data": default_config_data(&engine),
                })],
            );
            broker.insert(
                "configurations".into(),
                json!({ "current": { "id": cid, "revision": 1 }, "history": [] }),
            );
            (cid, 1)
        } else {
            (String::new(), 0)
        };

        acct.brokers.insert(id.clone(), Value::Object(broker));
        acct.users
            .insert(id.clone(), std::collections::BTreeMap::new());
        let tags = mq_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn.clone(), tags);
        }

        Ok(broker_attributes(
            id.clone(),
            arn,
            &engine,
            &region,
            &deployment,
            &config_id,
            config_rev,
        ))
    }

    pub(super) fn update_mq_broker(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        let new_name = mq_str(props, "BrokerName").unwrap_or_else(|| existing.logical_id.clone());
        let new_engine = mq_str(props, "EngineType")
            .unwrap_or_else(|| "ACTIVEMQ".to_string())
            .to_uppercase();
        let new_deployment =
            mq_str(props, "DeploymentMode").unwrap_or_else(|| "SINGLE_INSTANCE".to_string());

        let (old_name, old_engine, old_deployment) = {
            let guard = self.mq_state.read();
            let b = guard.get(&self.account_id).and_then(|a| a.brokers.get(&id));
            (
                b.and_then(|b| b.get("brokerName"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                b.and_then(|b| b.get("engineType"))
                    .and_then(Value::as_str)
                    .unwrap_or("ACTIVEMQ")
                    .to_string(),
                b.and_then(|b| b.get("deploymentMode"))
                    .and_then(Value::as_str)
                    .unwrap_or("SINGLE_INSTANCE")
                    .to_string(),
            )
        };
        // BrokerName, EngineType, and DeploymentMode are replacement-required.
        if new_name != old_name || new_engine != old_engine || new_deployment != old_deployment {
            self.delete_mq_broker(&id);
            return self.create_mq_broker(resource);
        }

        let region = self.region.clone();
        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let arn = acct
            .brokers
            .get(&id)
            .and_then(|b| b.get("brokerArn"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let (config_id, config_rev) = {
            let broker = acct
                .brokers
                .get_mut(&id)
                .ok_or_else(|| format!("MQ broker {id} not yet provisioned"))?;
            let obj = broker.as_object_mut().expect("broker is an object");
            if let Some(v) = mq_str(props, "HostInstanceType") {
                obj.insert("hostInstanceType".into(), json!(v));
            }
            if let Some(v) = mq_str(props, "EngineVersion") {
                obj.insert("engineVersion".into(), json!(v));
            }
            if props.get("SecurityGroups").is_some() {
                obj.insert(
                    "securityGroups".into(),
                    mq_string_list(props, "SecurityGroups"),
                );
            }
            obj.insert(
                "autoMinorVersionUpgrade".into(),
                json!(mq_bool(props, "AutoMinorVersionUpgrade")),
            );
            if let Some(cfg) = props.get("Configuration") {
                let cid = cfg
                    .get("Id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let rev = cfg.get("Revision").and_then(Value::as_i64).unwrap_or(1);
                obj.insert(
                    "configurations".into(),
                    json!({ "current": { "id": cid, "revision": rev }, "history": [] }),
                );
            }
            let configs = obj.get("configurations");
            (
                configs
                    .and_then(|c| c.get("current"))
                    .and_then(|c| c.get("id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                configs
                    .and_then(|c| c.get("current"))
                    .and_then(|c| c.get("revision"))
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            )
        };
        let tags = mq_tags(props);
        if tags.is_empty() {
            acct.tags.remove(&arn);
        } else {
            acct.tags.insert(arn.clone(), tags);
        }
        Ok(broker_attributes(
            id,
            arn,
            &old_engine,
            &region,
            &old_deployment,
            &config_id,
            config_rev,
        ))
    }

    pub(super) fn get_att_mq_broker(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let guard = self.mq_state.read();
        let acct = guard.get(&self.account_id)?;
        let b = acct.brokers.get(physical_id)?;
        if attribute == "Arn" {
            return b
                .get("brokerArn")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        let engine = b
            .get("engineType")
            .and_then(Value::as_str)
            .unwrap_or("ACTIVEMQ");
        let deployment = b
            .get("deploymentMode")
            .and_then(Value::as_str)
            .unwrap_or("SINGLE_INSTANCE");
        let cfg = b.get("configurations").and_then(|c| c.get("current"));
        let cid = cfg
            .and_then(|c| c.get("id"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let crev = cfg
            .and_then(|c| c.get("revision"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let res = broker_attributes(
            physical_id.to_string(),
            b.get("brokerArn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            engine,
            &self.region,
            deployment,
            &cid,
            crev,
        );
        res.attributes.get(attribute).cloned()
    }

    pub(super) fn delete_mq_broker(&self, physical_id: &str) {
        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if let Some(b) = acct.brokers.remove(physical_id) {
            if let Some(arn) = b.get("brokerArn").and_then(Value::as_str) {
                acct.tags.remove(arn);
            }
        }
        acct.users.remove(physical_id);
    }

    // -------------------------------------------------------- Configuration

    pub(super) fn create_mq_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = mq_str(props, "Name").unwrap_or_else(|| resource.logical_id.clone());
        let engine = mq_str(props, "EngineType")
            .unwrap_or_else(|| "ACTIVEMQ".to_string())
            .to_uppercase();
        let engine_version = mq_str(props, "EngineVersion")
            .unwrap_or_else(|| default_engine_version(&engine).to_string());
        let auth = mq_str(props, "AuthenticationStrategy").unwrap_or_else(|| "SIMPLE".to_string());
        let description = mq_str(props, "Description").unwrap_or_default();
        let data = mq_str(props, "Data").unwrap_or_else(|| default_config_data(&engine));
        let region = self.region.clone();
        let account = self.account_id.clone();
        let id = format!("c-{}", Uuid::new_v4());
        let arn = format!("arn:aws:mq:{region}:{account}:configuration:{id}");
        let created = now_iso();

        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        acct.configurations.insert(
            id.clone(),
            json!({
                "arn": arn,
                "authenticationStrategy": auth,
                "created": created,
                "description": description,
                "engineType": engine,
                "engineVersion": engine_version,
                "id": id,
                "latestRevision": { "revision": 1, "created": created, "description": description },
                "name": name,
            }),
        );
        acct.configuration_revisions.insert(
            id.clone(),
            vec![json!({ "revision": 1, "created": created, "description": description, "data": data })],
        );
        let tags = mq_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn.clone(), tags);
        }

        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Revision", "1"))
    }

    pub(super) fn update_mq_configuration(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = existing.physical_id.clone();
        // EngineType is replacement-required; a change re-provisions from scratch.
        let new_engine = mq_str(props, "EngineType")
            .unwrap_or_else(|| "ACTIVEMQ".to_string())
            .to_uppercase();
        let old_engine = {
            let guard = self.mq_state.read();
            guard
                .get(&self.account_id)
                .and_then(|a| a.configurations.get(&id))
                .and_then(|c| c.get("engineType"))
                .and_then(Value::as_str)
                .unwrap_or("ACTIVEMQ")
                .to_string()
        };
        if new_engine != old_engine {
            self.delete_mq_configuration(&id);
            return self.create_mq_configuration(resource);
        }

        let description = mq_str(props, "Description").unwrap_or_default();
        let data = mq_str(props, "Data");
        let created = now_iso();
        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let arn = {
            let revs = acct.configuration_revisions.entry(id.clone()).or_default();
            let next = revs
                .iter()
                .filter_map(|r| r.get("revision").and_then(Value::as_i64))
                .max()
                .unwrap_or(0)
                + 1;
            revs.push(json!({
                "revision": next,
                "created": created,
                "description": description,
                "data": data.clone().unwrap_or_else(|| default_config_data(&old_engine)),
            }));
            let cfg = acct
                .configurations
                .get_mut(&id)
                .ok_or_else(|| format!("MQ configuration {id} not yet provisioned"))?;
            cfg["latestRevision"] =
                json!({ "revision": next, "created": created, "description": description });
            let arn = cfg
                .get("arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let rev_str = next.to_string();
            let _ = rev_str;
            (arn, next)
        };
        let tags = mq_tags(props);
        if tags.is_empty() {
            acct.tags.remove(&arn.0);
        } else {
            acct.tags.insert(arn.0.clone(), tags);
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn.0)
            .with("Id", id)
            .with("Revision", arn.1.to_string()))
    }

    pub(super) fn get_att_mq_configuration(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.mq_state.read();
        let c = guard
            .get(&self.account_id)?
            .configurations
            .get(physical_id)?;
        match attribute {
            "Arn" => c.get("arn").and_then(Value::as_str).map(str::to_string),
            "Id" => Some(physical_id.to_string()),
            "Revision" => c
                .get("latestRevision")
                .and_then(|r| r.get("revision"))
                .and_then(Value::as_i64)
                .map(|n| n.to_string()),
            _ => None,
        }
    }

    pub(super) fn delete_mq_configuration(&self, physical_id: &str) {
        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if let Some(c) = acct.configurations.remove(physical_id) {
            if let Some(arn) = c.get("arn").and_then(Value::as_str) {
                acct.tags.remove(arn);
            }
        }
        acct.configuration_revisions.remove(physical_id);
    }

    // --------------------------------------------- ConfigurationAssociation

    pub(super) fn create_mq_configuration_association(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let broker_id = mq_str(props, "Broker")
            .ok_or_else(|| "AWS::AmazonMQ::ConfigurationAssociation requires Broker".to_string())?;
        let cfg = props.get("Configuration").ok_or_else(|| {
            "AWS::AmazonMQ::ConfigurationAssociation requires Configuration".to_string()
        })?;
        let cid = cfg
            .get("Id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let rev = cfg.get("Revision").and_then(Value::as_i64).unwrap_or(1);

        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let broker = acct
            .brokers
            .get_mut(&broker_id)
            .ok_or_else(|| format!("MQ broker {broker_id} does not exist"))?;
        if let Some(obj) = broker.as_object_mut() {
            obj.insert(
                "configurations".into(),
                json!({ "current": { "id": cid, "revision": rev }, "history": [] }),
            );
        }
        // The association has no independent identity; Ref resolves to the broker.
        Ok(ProvisionResult::new(broker_id.clone()).with("Id", broker_id))
    }
}

/// Assemble a broker's `Fn::GetAtt` attribute set from its live wire endpoints.
fn broker_attributes(
    id: String,
    arn: String,
    engine: &str,
    region: &str,
    deployment_mode: &str,
    config_id: &str,
    config_revision: i64,
) -> ProvisionResult {
    let count = match (engine, deployment_mode) {
        ("ACTIVEMQ", "ACTIVE_STANDBY_MULTI_AZ") => 2,
        _ => 1,
    };
    let mut ips = Vec::new();
    let mut open_wire = Vec::new();
    let mut amqp = Vec::new();
    let mut stomp = Vec::new();
    let mut mqtt = Vec::new();
    let mut wss = Vec::new();
    for i in 1..=count {
        let h = fnv1a(&format!("{id}-{i}"));
        ips.push(format!(
            "10.{}.{}.{}",
            (h >> 16) & 0xff,
            (h >> 8) & 0xff,
            h & 0xff
        ));
        if engine == "RABBITMQ" {
            let host = format!("{id}.mq.{region}.amazonaws.com");
            amqp.push(format!("amqps://{host}:5671"));
        } else {
            let host = format!("{id}-{i}.mq.{region}.amazonaws.com");
            open_wire.push(format!("ssl://{host}:61617"));
            amqp.push(format!("amqp+ssl://{host}:5671"));
            stomp.push(format!("stomp+ssl://{host}:61614"));
            mqtt.push(format!("mqtt+ssl://{host}:8883"));
            wss.push(format!("wss://{host}:61619"));
        }
    }
    // `attributes` is String->String; list attributes resolve to their first
    // element (the value a bare `Fn::GetAtt` without an `Fn::Select` needs).
    let mut res = ProvisionResult::new(id).with("Arn", arn);
    if let Some(v) = ips.first() {
        res = res.with("IpAddresses", v.clone());
    }
    if let Some(v) = open_wire.first() {
        res = res.with("OpenWireEndpoints", v.clone());
    }
    if let Some(v) = amqp.first() {
        res = res.with("AmqpEndpoints", v.clone());
    }
    if let Some(v) = stomp.first() {
        res = res.with("StompEndpoints", v.clone());
    }
    if let Some(v) = mqtt.first() {
        res = res.with("MqttEndpoints", v.clone());
    }
    if let Some(v) = wss.first() {
        res = res.with("WssEndpoints", v.clone());
    }
    if !config_id.is_empty() {
        res = res
            .with("ConfigurationId", config_id.to_string())
            .with("ConfigurationRevision", config_revision.to_string());
    }
    res
}

fn mq_str(props: &Value, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn mq_bool(props: &Value, key: &str) -> bool {
    match props.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

fn mq_string_list(props: &Value, key: &str) -> Value {
    let mut out = Vec::new();
    if let Some(arr) = props.get(key).and_then(Value::as_array) {
        for v in arr {
            if let Some(s) = v.as_str() {
                out.push(json!(s));
            }
        }
    }
    Value::Array(out)
}

/// Convert CFN `Tags` (`[{Key,Value}]`) into the `mq` tag map.
fn mq_tags(props: &Value) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(arr) = props.get("Tags").and_then(Value::as_array) {
        for t in arr {
            let key = t.get("Key").and_then(Value::as_str).unwrap_or("");
            let value = t.get("Value").and_then(Value::as_str).unwrap_or("");
            if !key.is_empty() {
                out.insert(key.to_string(), value.to_string());
            }
        }
    }
    out
}

fn default_engine_version(engine: &str) -> &'static str {
    if engine == "RABBITMQ" {
        "3.13"
    } else {
        "5.18"
    }
}

fn default_config_data(engine: &str) -> String {
    let xml = if engine == "RABBITMQ" {
        "consumer_timeout = 1800000\n".to_string()
    } else {
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<broker xmlns=\"http://activemq.apache.org/schema/core\" start=\"false\">\n</broker>\n".to_string()
    };
    base64::engine::general_purpose::STANDARD.encode(xml.as_bytes())
}

fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
