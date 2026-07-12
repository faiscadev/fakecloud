//! `AWS::MSK::*` CloudFormation provisioning (Amazon MSK / `kafka`).
//!
//! Each resource is written through to the `kafka` service state as the same
//! wire JSON the direct `Create*` handlers store -- via the shared
//! `fakecloud_kafka::builders` module -- so a CFN-created cluster / configuration
//! / replicator / VPC connection reads back identically on `Describe*` and
//! persists through the `kafka` snapshot hook (survives a restart -- the #1766
//! phantom-resource lesson). A provisioned/serverless cluster is inserted
//! synchronously (so `Ref` / `Fn::GetAtt` resolve during provisioning) with
//! `state` `CREATING`; a PROVISIONED cluster is then backed by a REAL Apache
//! Kafka container in the background (drained after provisioning, #1539/#1730),
//! settling to `ACTIVE` once it serves. Without a runtime (CI / metadata-only)
//! or for a serverless cluster the in-memory reconcile settles it at once, the
//! same as the direct API.
//!
//! Physical id + `Ref` / `Fn::GetAtt` (verified against the AWS resource spec):
//!   Cluster           -> Ref = cluster ARN; GetAtt Arn
//!   ServerlessCluster -> Ref = cluster ARN; GetAtt Arn
//!   Configuration     -> Ref = configuration ARN; GetAtt Arn
//!   ClusterPolicy     -> Ref = cluster ARN; GetAtt CurrentVersion
//!   BatchScramSecret  -> Ref = cluster ARN
//!   VpcConnection     -> Ref = VPC-connection ARN; GetAtt Arn
//!   Replicator        -> Ref = replicator ARN; GetAtt ReplicatorArn

use serde_json::{json, Map, Value};

use fakecloud_kafka::builders;

use super::{ContainerSpawnIntent, ContainerTeardownIntent, ProvisionResult, ResourceDefinition};

impl super::ResourceProvisioner {
    // ---------------------------------------------------------------- Cluster

    pub(super) fn create_msk_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut body = pascal_to_camel_without_tags(props);
        set_body_tags(&mut body, props);
        self.insert_and_settle_cluster(&body, false)
    }

    pub(super) fn create_msk_serverless_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        // The serverless resource carries its shape under a `serverless` block in
        // the CreateClusterV2 body the builder consumes.
        let mut serverless = Map::new();
        if let Some(v) = props.get("VpcConfigs") {
            serverless.insert("vpcConfigs".into(), pascal_to_camel(v));
        }
        if let Some(v) = props.get("ClientAuthentication") {
            serverless.insert("clientAuthentication".into(), pascal_to_camel(v));
        }
        let mut body = json!({
            "clusterName": msk_str(props, "ClusterName").unwrap_or_default(),
            "serverless": Value::Object(serverless),
        });
        set_body_tags(&mut body, props);
        self.insert_and_settle_cluster(&body, true)
    }

    /// Insert a cluster through the shared builder, then settle it: background a
    /// real container for a PROVISIONED cluster when a runtime is present, else
    /// reconcile it straight to ACTIVE in memory (serverless, or no runtime).
    fn insert_and_settle_cluster(&self, body: &Value, v2: bool) -> Result<ProvisionResult, String> {
        let serverless = v2 && body.get("serverless").is_some();
        let runtime_present = self.kafka_runtime.is_some();

        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        let arn = builders::insert_cluster(data, &self.region, &self.account_id, body, v2)?;

        if runtime_present && !serverless {
            self.pending_container_spawns
                .lock()
                .push(ContainerSpawnIntent::MskCluster {
                    cluster_arn: arn.clone(),
                });
        } else {
            // Serverless (no broker to run) and the no-runtime fallback settle
            // via the in-memory state machine, exactly as the direct API does.
            data.reconcile(runtime_present);
        }

        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn update_msk_cluster(
        &self,
        existing: &super::StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let new_name = msk_str(props, "ClusterName");

        let old_name = {
            let guard = self.kafka_state.read();
            guard
                .get(&self.account_id)
                .and_then(|d| d.clusters.get(&arn))
                .and_then(|c| c.get("clusterName"))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        // ClusterName is create-only: a change replaces the cluster.
        if let (Some(new), Some(old)) = (new_name.as_deref(), old_name.as_deref()) {
            if new != old {
                self.delete_msk_cluster(&arn);
                return self.create_msk_cluster(resource);
            }
        }

        // Otherwise mutate the in-place-updatable members (broker count, Kafka
        // version, monitoring, storage mode, encryption, broker group info) and
        // re-tag, keeping the ARN / physical id stable.
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        let cluster = data
            .clusters
            .get_mut(&arn)
            .ok_or_else(|| format!("MSK cluster {arn} not yet provisioned"))?;
        if let Some(obj) = cluster.as_object_mut() {
            if let Some(v) = props.get("NumberOfBrokerNodes") {
                obj.insert("numberOfBrokerNodes".into(), v.clone());
            }
            if let Some(v) = msk_str(props, "KafkaVersion") {
                obj.insert(
                    "currentBrokerSoftwareInfo".into(),
                    json!({ "kafkaVersion": v }),
                );
            }
            if let Some(v) = props.get("EnhancedMonitoring") {
                obj.insert("enhancedMonitoring".into(), v.clone());
            }
            if let Some(v) = msk_str(props, "StorageMode") {
                obj.insert("storageMode".into(), json!(v));
            }
            if let Some(v) = props.get("EncryptionInfo") {
                obj.insert("encryptionInfo".into(), pascal_to_camel(v));
            }
            if let Some(v) = props.get("BrokerNodeGroupInfo") {
                obj.insert("brokerNodeGroupInfo".into(), pascal_to_camel(v));
            }
        }
        set_state_tags(data, &arn, props);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn get_att_msk_cluster(&self, physical_id: &str, attribute: &str) -> Option<String> {
        // Cluster Ref = ARN (= physical id). GetAtt Arn is the same ARN.
        match attribute {
            "Arn" => {
                let guard = self.kafka_state.read();
                guard
                    .get(&self.account_id)?
                    .clusters
                    .get(physical_id)
                    .map(|_| physical_id.to_string())
            }
            _ => None,
        }
    }

    pub(super) fn delete_msk_cluster(&self, physical_id: &str) {
        {
            let mut guard = self.kafka_state.write();
            let data = guard.get_or_create(&self.account_id);
            if data.clusters.remove(physical_id).is_some() {
                data.tags.remove(physical_id);
            }
            data.topics.remove(physical_id);
            data.scram_secrets.remove(physical_id);
            data.policies.remove(physical_id);
            data.data_plane.remove(physical_id);
        }
        // Reap the REAL backing container (if any) off the request path.
        if self.kafka_runtime.is_some() {
            self.pending_container_teardowns
                .lock()
                .push(ContainerTeardownIntent::MskCluster {
                    cluster_arn: physical_id.to_string(),
                });
        }
    }

    // ---------------------------------------------------------- Configuration

    pub(super) fn create_msk_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let body = json!({
            "name": msk_str(props, "Name").unwrap_or_else(|| resource.logical_id.clone()),
            "description": msk_str(props, "Description").unwrap_or_default(),
            "serverProperties": msk_str(props, "ServerProperties").unwrap_or_default(),
            "kafkaVersions": props.get("KafkaVersionsList").cloned().unwrap_or(json!(null)),
        });
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        let arn = builders::insert_configuration(data, &self.region, &self.account_id, &body);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn update_msk_configuration(
        &self,
        existing: &super::StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let description = msk_str(props, "Description").unwrap_or_default();
        let server_properties = msk_str(props, "ServerProperties").unwrap_or_default();
        let created = fakecloud_kafka::shared::now_iso();

        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        // Appending a revision matches ServerProperties being update-with-a-new-
        // revision on the real resource; missing config -> re-provision.
        let revs = data.configuration_revisions.entry(arn.clone()).or_default();
        let next = revs
            .iter()
            .filter_map(|r| r.get("revision").and_then(Value::as_i64))
            .max()
            .unwrap_or(0)
            + 1;
        revs.push(json!({
            "creationTime": created,
            "description": description,
            "revision": next,
            "serverProperties": server_properties,
        }));
        let cfg = data
            .configurations
            .get_mut(&arn)
            .ok_or_else(|| format!("MSK configuration {arn} not yet provisioned"))?;
        if let Some(obj) = cfg.as_object_mut() {
            obj.insert(
                "latestRevision".into(),
                json!({ "creationTime": created, "description": description, "revision": next }),
            );
        }
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn get_att_msk_configuration(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        match attribute {
            "Arn" => {
                let guard = self.kafka_state.read();
                guard
                    .get(&self.account_id)?
                    .configurations
                    .get(physical_id)
                    .map(|_| physical_id.to_string())
            }
            _ => None,
        }
    }

    pub(super) fn delete_msk_configuration(&self, physical_id: &str) {
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.configurations.remove(physical_id);
        data.configuration_revisions.remove(physical_id);
        data.tags.remove(physical_id);
    }

    // --------------------------------------------------------- ClusterPolicy

    pub(super) fn create_msk_cluster_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_arn = msk_str(props, "ClusterArn")
            .ok_or_else(|| "AWS::MSK::ClusterPolicy requires ClusterArn".to_string())?;
        // Policy accepts either an inline JSON object or an already-serialized
        // string; store the serialized form the API's GetClusterPolicy returns.
        let policy = match props.get("Policy") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => return Err("AWS::MSK::ClusterPolicy requires Policy".to_string()),
        };
        let version = "1".to_string();
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        if !data.clusters.contains_key(&cluster_arn) {
            return Err(format!("The cluster '{cluster_arn}' does not exist."));
        }
        data.policies.insert(
            cluster_arn.clone(),
            json!({ "policy": policy, "version": version }),
        );
        // Ref = the cluster ARN; GetAtt CurrentVersion = the policy version.
        Ok(ProvisionResult::new(cluster_arn).with("CurrentVersion", version))
    }

    pub(super) fn update_msk_cluster_policy(
        &self,
        existing: &super::StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // The physical id is the cluster ARN; re-put the policy (a new version).
        let arn = existing.physical_id.clone();
        let props = &resource.properties;
        let policy = match props.get("Policy") {
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => return Err("AWS::MSK::ClusterPolicy requires Policy".to_string()),
        };
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        let prev = data
            .policies
            .get(&arn)
            .and_then(|p| p.get("version"))
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1);
        let version = (prev + 1).to_string();
        data.policies
            .insert(arn.clone(), json!({ "policy": policy, "version": version }));
        Ok(ProvisionResult::new(arn).with("CurrentVersion", version))
    }

    pub(super) fn get_att_msk_cluster_policy(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        match attribute {
            "CurrentVersion" => {
                let guard = self.kafka_state.read();
                guard
                    .get(&self.account_id)?
                    .policies
                    .get(physical_id)?
                    .get("version")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }
            _ => None,
        }
    }

    pub(super) fn delete_msk_cluster_policy(&self, physical_id: &str) {
        let mut guard = self.kafka_state.write();
        guard
            .get_or_create(&self.account_id)
            .policies
            .remove(physical_id);
    }

    // -------------------------------------------------------- BatchScramSecret

    pub(super) fn create_msk_batch_scram_secret(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_arn = msk_str(props, "ClusterArn")
            .ok_or_else(|| "AWS::MSK::BatchScramSecret requires ClusterArn".to_string())?;
        let secrets = msk_string_list(props, "SecretArnList");
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        if !data.clusters.contains_key(&cluster_arn) {
            return Err(format!("The cluster '{cluster_arn}' does not exist."));
        }
        let entry = data.scram_secrets.entry(cluster_arn.clone()).or_default();
        for s in secrets {
            if !entry.contains(&s) {
                entry.push(s);
            }
        }
        // The association has no independent identity; Ref resolves to the cluster.
        Ok(ProvisionResult::new(cluster_arn))
    }

    pub(super) fn update_msk_batch_scram_secret(
        &self,
        existing: &super::StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Physical id is the cluster ARN; reconcile the associated set to the new
        // SecretArnList (the resource fully owns the association).
        let arn = existing.physical_id.clone();
        let secrets = msk_string_list(&resource.properties, "SecretArnList");
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.scram_secrets.insert(arn.clone(), secrets);
        Ok(ProvisionResult::new(arn))
    }

    pub(super) fn delete_msk_batch_scram_secret(&self, physical_id: &str) {
        let mut guard = self.kafka_state.write();
        guard
            .get_or_create(&self.account_id)
            .scram_secrets
            .remove(physical_id);
    }

    // ---------------------------------------------------------- VpcConnection

    pub(super) fn create_msk_vpc_connection(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut body = json!({
            "authentication": msk_str(props, "Authentication").unwrap_or_default(),
            "clientSubnets": msk_string_list(props, "ClientSubnets"),
            "securityGroups": msk_string_list(props, "SecurityGroups"),
            "targetClusterArn": msk_str(props, "TargetClusterArn").unwrap_or_default(),
            "vpcId": msk_str(props, "VpcId").unwrap_or_default(),
        });
        set_body_tags(&mut body, props);
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        let arn = builders::insert_vpc_connection(data, &self.region, &self.account_id, &body);
        // A VPC connection settles to AVAILABLE on the next describe; reconcile
        // it now so a metadata-only stack reaches a stable state at once.
        data.reconcile(self.kafka_runtime.is_some());
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn update_msk_vpc_connection(
        &self,
        existing: &super::StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // A VPC connection's members are all create-only, so any change is a
        // replacement: drop the old record (new ARN) and re-provision.
        self.delete_msk_vpc_connection(&existing.physical_id);
        self.create_msk_vpc_connection(resource)
    }

    pub(super) fn get_att_msk_vpc_connection(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        match attribute {
            "Arn" => {
                let guard = self.kafka_state.read();
                guard
                    .get(&self.account_id)?
                    .vpc_connections
                    .get(physical_id)
                    .map(|_| physical_id.to_string())
            }
            _ => None,
        }
    }

    pub(super) fn delete_msk_vpc_connection(&self, physical_id: &str) {
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.vpc_connections.remove(physical_id);
        data.tags.remove(physical_id);
    }

    // ------------------------------------------------------------- Replicator

    pub(super) fn create_msk_replicator(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let mut body = json!({
            "replicatorName": msk_str(props, "ReplicatorName").unwrap_or_else(|| resource.logical_id.clone()),
            "description": msk_str(props, "Description").unwrap_or_default(),
            "serviceExecutionRoleArn": msk_str(props, "ServiceExecutionRoleArn").unwrap_or_default(),
            "kafkaClusters": pascal_to_camel(props.get("KafkaClusters").unwrap_or(&json!([]))),
            "replicationInfoList": pascal_to_camel(props.get("ReplicationInfoList").unwrap_or(&json!([]))),
        });
        set_body_tags(&mut body, props);
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        let arn = builders::insert_replicator(data, &self.region, &self.account_id, &body)?;
        // Replicators settle CREATING -> RUNNING on the next describe.
        data.reconcile(self.kafka_runtime.is_some());
        Ok(ProvisionResult::new(arn.clone()).with("ReplicatorArn", arn))
    }

    pub(super) fn update_msk_replicator(
        &self,
        existing: &super::StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let arn = existing.physical_id.clone();
        let new_name = msk_str(props, "ReplicatorName");
        let old_name = {
            let guard = self.kafka_state.read();
            guard
                .get(&self.account_id)
                .and_then(|d| d.replicators.get(&arn))
                .and_then(|r| r.get("replicatorName"))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        // ReplicatorName is create-only.
        if let (Some(new), Some(old)) = (new_name.as_deref(), old_name.as_deref()) {
            if new != old {
                self.delete_msk_replicator(&arn);
                return self.create_msk_replicator(resource);
            }
        }
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        if let Some(obj) = data
            .replicators
            .get_mut(&arn)
            .and_then(Value::as_object_mut)
        {
            if let Some(v) = props.get("ReplicationInfoList") {
                obj.insert("replicationInfoList".into(), pascal_to_camel(v));
            }
            if let Some(v) = msk_str(props, "Description") {
                obj.insert("replicatorDescription".into(), json!(v));
            }
        }
        set_state_tags(data, &arn, props);
        Ok(ProvisionResult::new(arn.clone()).with("ReplicatorArn", arn))
    }

    pub(super) fn get_att_msk_replicator(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        match attribute {
            // Ref and ReplicatorArn are both the replicator ARN (= physical id).
            "ReplicatorArn" => {
                let guard = self.kafka_state.read();
                guard
                    .get(&self.account_id)?
                    .replicators
                    .get(physical_id)
                    .map(|_| physical_id.to_string())
            }
            _ => None,
        }
    }

    pub(super) fn delete_msk_replicator(&self, physical_id: &str) {
        let mut guard = self.kafka_state.write();
        let data = guard.get_or_create(&self.account_id);
        data.replicators.remove(physical_id);
        data.tags.remove(physical_id);
    }
}

// ---------------------------------------------------------------- helpers

fn msk_str(props: &Value, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn msk_string_list(props: &Value, key: &str) -> Vec<String> {
    props
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Recursively convert CloudFormation PascalCase object keys to the MSK wire
/// camelCase the service state stores (`InstanceType` -> `instanceType`), so a
/// CFN-provisioned resource reads back with the same key casing the direct API
/// uses. Values (including tag *values* and free-form strings) are untouched.
fn pascal_to_camel(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, val)| (pascal_key_to_camel(k), pascal_to_camel(val)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(pascal_to_camel).collect()),
        other => other.clone(),
    }
}

fn pascal_key_to_camel(k: &str) -> String {
    match k {
        // Acronym-leading key whose MSK wire form isn't a plain first-letter
        // lowercasing (`EBSStorageInfo` -> `ebsStorageInfo`, not `eBSStorageInfo`).
        "EBSStorageInfo" => "ebsStorageInfo".to_string(),
        _ => {
            let mut c = k.chars();
            match c.next() {
                Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
                None => String::new(),
            }
        }
    }
}

/// Build the top-level CreateCluster/CreateVpcConnection body from CFN props by
/// camel-casing every property EXCEPT `Tags` (whose keys are user data and must
/// not be case-folded).
fn pascal_to_camel_without_tags(props: &Value) -> Value {
    let mut out = Map::new();
    if let Some(m) = props.as_object() {
        for (k, v) in m {
            if k == "Tags" {
                continue;
            }
            out.insert(pascal_key_to_camel(k), pascal_to_camel(v));
        }
    }
    Value::Object(out)
}

/// CFN MSK `Tags` -> the `{k: v}` object the shared builders read. MSK models
/// tags as a key-value MAP, but accept the `[{Key, Value}]` list form too for
/// robustness.
fn cfn_tags_to_object(props: &Value) -> Value {
    match props.get("Tags") {
        Some(Value::Object(m)) => {
            let obj: Map<String, Value> = m
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), json!(s))))
                .collect();
            Value::Object(obj)
        }
        Some(Value::Array(a)) => {
            let obj: Map<String, Value> = a
                .iter()
                .filter_map(|t| {
                    let k = t.get("Key").and_then(Value::as_str)?;
                    let v = t.get("Value").and_then(Value::as_str).unwrap_or("");
                    Some((k.to_string(), json!(v)))
                })
                .collect();
            Value::Object(obj)
        }
        _ => json!({}),
    }
}

/// Set the `tags` member on a builder body from CFN `Tags`, if any.
fn set_body_tags(body: &mut Value, props: &Value) {
    let tags = cfn_tags_to_object(props);
    if tags.as_object().is_some_and(|m| !m.is_empty()) {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("tags".into(), tags);
        }
    }
}

/// Replace the ARN-keyed tag map for `arn` from CFN `Tags` on an update.
fn set_state_tags(data: &mut fakecloud_kafka::KafkaData, arn: &str, props: &Value) {
    let tags = cfn_tags_to_object(props);
    let map: std::collections::BTreeMap<String, String> = tags
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    if map.is_empty() {
        data.tags.remove(arn);
    } else {
        data.tags.insert(arn.to_string(), map);
    }
}
