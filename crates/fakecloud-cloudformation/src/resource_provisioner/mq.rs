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

use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_mq::shared as mq_shared;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

impl ResourceProvisioner {
    // -------------------------------------------------------------- Broker

    pub(super) fn create_mq_broker(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = mq_str(props, "BrokerName").unwrap_or_else(|| resource.logical_id.clone());
        let region = self.region.clone();
        let account = self.account_id.clone();

        // Translate the CFN PascalCase properties into a restJson1 `CreateBroker`
        // body and go through the exact same shared create path the direct API
        // uses, so a CFN-created broker is byte-for-byte identical (users,
        // synthesized subnets, auto-configuration, logs, encryption, tags) --
        // the two paths cannot diverge (#1766).
        let body = cfn_broker_body(props, &name);
        let engine = body
            .get("engineType")
            .and_then(Value::as_str)
            .unwrap_or("ACTIVEMQ")
            .to_string();
        let deployment = body
            .get("deploymentMode")
            .and_then(Value::as_str)
            .unwrap_or("SINGLE_INSTANCE")
            .to_string();

        let mut guard = self.mq_state.write();
        let acct = guard.get_or_create(&self.account_id);
        // Broker names are unique per account+region (matching the API handler).
        if acct.brokers.values().any(|b| {
            b.get("brokerName").and_then(Value::as_str) == Some(name.as_str())
                && mq_shared::broker_region(b) == Some(region.as_str())
        }) {
            return Err(format!("Broker name {name} already exists"));
        }

        let (id, arn) = mq_shared::create_broker_record(acct, &account, &region, &body);
        // CloudFormation blocks CREATE_COMPLETE until the broker is RUNNING, so
        // settle the freshly-created broker through the in-memory lifecycle
        // (auto_settle = true); the CFN provisioner is synchronous and does not
        // spawn a backing container.
        acct.reconcile_brokers(true);

        let (config_id, config_rev) = broker_current_config(acct.brokers.get(&id));
        Ok(broker_attributes(
            id,
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
                // Preserve configuration history: push the prior current onto
                // history rather than discarding it.
                mq_shared::set_broker_configuration(obj, &cid, rev);
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
            .unwrap_or_else(|| mq_shared::default_engine_version(&engine).to_string());
        let auth = mq_str(props, "AuthenticationStrategy").unwrap_or_else(|| "SIMPLE".to_string());
        let description = mq_str(props, "Description").unwrap_or_default();
        let data = mq_str(props, "Data").unwrap_or_else(|| mq_shared::default_config_data(&engine));
        let region = self.region.clone();
        let account = self.account_id.clone();
        let id = format!("c-{}", Uuid::new_v4());
        let arn = mq_shared::config_arn(&region, &account, &id);
        let created = mq_shared::now_iso();

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
        let created = mq_shared::now_iso();
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
                "data": data.clone().unwrap_or_else(|| mq_shared::default_config_data(&old_engine)),
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
            // Preserve the broker's configuration history: the prior current
            // configuration is pushed onto history, not discarded.
            mq_shared::set_broker_configuration(obj, &cid, rev);
        }
        // The association has no independent identity; Ref resolves to the broker.
        Ok(ProvisionResult::new(broker_id.clone()).with("Id", broker_id))
    }
}

/// Assemble a broker's `Fn::GetAtt` attribute set from its live wire endpoints
/// (projected from the single `mq_shared::broker_endpoints` source). The
/// list-valued attributes (`IpAddresses`, `*Endpoints`) are stored as JSON
/// arrays so `Fn::Select` / list-typed `Fn::GetAtt` resolve them as real lists
/// rather than collapsing to a scalar.
fn broker_attributes(
    id: String,
    arn: String,
    engine: &str,
    region: &str,
    deployment_mode: &str,
    config_id: &str,
    config_revision: i64,
) -> ProvisionResult {
    let e = mq_shared::broker_endpoints(&id, engine, region, deployment_mode, None);
    let mut res = ProvisionResult::new(id)
        .with("Arn", arn)
        .with("IpAddresses", json_list(&e.ips))
        .with("OpenWireEndpoints", json_list(&e.open_wire))
        .with("AmqpEndpoints", json_list(&e.amqp))
        .with("StompEndpoints", json_list(&e.stomp))
        .with("MqttEndpoints", json_list(&e.mqtt))
        .with("WssEndpoints", json_list(&e.wss));
    if !config_id.is_empty() {
        res = res
            .with("ConfigurationId", config_id.to_string())
            .with("ConfigurationRevision", config_revision.to_string());
    }
    res
}

/// A list-valued `Fn::GetAtt` attribute, encoded as a JSON array string so the
/// intrinsic-function resolver can hand it back as a real list.
fn json_list(items: &[String]) -> String {
    serde_json::to_string(items).unwrap_or_else(|_| "[]".to_string())
}

/// The `{id, revision}` of a broker's current configuration (empty id / 0 when
/// the broker has none, e.g. RabbitMQ).
fn broker_current_config(broker: Option<&Value>) -> (String, i64) {
    let cur = broker
        .and_then(|b| b.get("configurations"))
        .and_then(|c| c.get("current"));
    let id = cur
        .and_then(|c| c.get("id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let rev = cur
        .and_then(|c| c.get("revision"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    (id, rev)
}

/// Translate an `AWS::AmazonMQ::Broker` resource's PascalCase properties into a
/// restJson1 `CreateBroker` request body (camelCase members) so the CFN path
/// can reuse the exact shared service create logic.
fn cfn_broker_body(props: &Value, name: &str) -> Value {
    let mut b = serde_json::Map::new();
    b.insert("brokerName".into(), json!(name));
    b.insert(
        "engineType".into(),
        json!(mq_str(props, "EngineType")
            .unwrap_or_else(|| "ACTIVEMQ".to_string())
            .to_uppercase()),
    );
    if let Some(v) = mq_str(props, "EngineVersion") {
        b.insert("engineVersion".into(), json!(v));
    }
    b.insert(
        "deploymentMode".into(),
        json!(mq_str(props, "DeploymentMode").unwrap_or_else(|| "SINGLE_INSTANCE".to_string())),
    );
    if let Some(v) = mq_str(props, "HostInstanceType") {
        b.insert("hostInstanceType".into(), json!(v));
    }
    b.insert(
        "publiclyAccessible".into(),
        json!(mq_bool(props, "PubliclyAccessible")),
    );
    b.insert(
        "autoMinorVersionUpgrade".into(),
        json!(mq_bool(props, "AutoMinorVersionUpgrade")),
    );
    if let Some(v) = mq_str(props, "StorageType") {
        b.insert("storageType".into(), json!(v));
    }
    if let Some(v) = mq_str(props, "AuthenticationStrategy") {
        b.insert("authenticationStrategy".into(), json!(v));
    }
    if props.get("SecurityGroups").is_some() {
        b.insert(
            "securityGroups".into(),
            mq_string_list(props, "SecurityGroups"),
        );
    }
    // Only forward explicit subnets; when omitted, the shared path synthesizes
    // default-VPC subnet ids exactly as the direct API does.
    if props
        .get("SubnetIds")
        .and_then(Value::as_array)
        .is_some_and(|a| !a.is_empty())
    {
        b.insert("subnetIds".into(), mq_string_list(props, "SubnetIds"));
    }
    if let Some(logs) = props.get("Logs") {
        b.insert(
            "logs".into(),
            json!({
                "audit": logs.get("Audit").and_then(Value::as_bool).unwrap_or(false),
                "general": logs.get("General").and_then(Value::as_bool).unwrap_or(false),
            }),
        );
    }
    if let Some(cfg) = props.get("Configuration") {
        b.insert(
            "configuration".into(),
            json!({
                "id": cfg.get("Id").and_then(Value::as_str).unwrap_or_default(),
                "revision": cfg.get("Revision").and_then(Value::as_i64).unwrap_or(1),
            }),
        );
    }
    if let Some(arr) = props.get("Users").and_then(Value::as_array) {
        let users: Vec<Value> = arr
            .iter()
            .map(|u| {
                json!({
                    "username": u.get("Username").and_then(Value::as_str).unwrap_or_default(),
                    "password": u.get("Password").cloned().unwrap_or(json!("")),
                    "consoleAccess": u.get("ConsoleAccess").and_then(Value::as_bool).unwrap_or(false),
                    "groups": u.get("Groups").cloned().unwrap_or(json!([])),
                    "replicationUser": u.get("ReplicationUser").and_then(Value::as_bool).unwrap_or(false),
                })
            })
            .collect();
        b.insert("users".into(), Value::Array(users));
    }
    let tags = mq_tags(props);
    if !tags.is_empty() {
        b.insert("tags".into(), json!(tags));
    }
    Value::Object(b)
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
