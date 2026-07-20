//! `AWS::EMR::Cluster` CloudFormation provisioning. The cluster is written
//! through to the `emr` service state as the same `Cluster`-shaped JSON the
//! direct `RunJobFlow` handler stores (keyed by the `j-...` cluster id, with the
//! id pushed onto `cluster_order` so `ListClusters` sees it), so a CFN-created
//! cluster reads back on `DescribeCluster`/`ListClusters` and persists through
//! the `emr` snapshot hook (survives a restart -- #1766 class).
//!
//! `Ref` resolves to the cluster id (the physical id); `Fn::GetAtt MasterPublicDNS`
//! to the master public DNS name.

use serde_json::{json, Value};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};

impl ResourceProvisioner {
    pub(super) fn create_emr_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let region = &self.region;
        let account = &self.account_id;
        let id = format!(
            "j-{}",
            uuid::Uuid::new_v4().simple().to_string()[..13].to_uppercase()
        );
        let arn = format!("arn:aws:elasticmapreduce:{region}:{account}:cluster/{id}");
        let master_dns = format!("ip-10-0-0-1.{region}.compute.internal");

        let mut cluster = serde_json::Map::new();
        cluster.insert("Id".to_string(), json!(id));
        cluster.insert("Name".to_string(), json!(name));
        cluster.insert(
            "Status".to_string(),
            json!({
                "State": "WAITING",
                "StateChangeReason": { "Message": "Cluster ready to run steps." },
                "Timeline": {},
            }),
        );
        cluster.insert("ClusterArn".to_string(), json!(arn.clone()));
        cluster.insert("MasterPublicDnsName".to_string(), json!(master_dns.clone()));
        cluster.insert("AutoTerminate".to_string(), json!(false));
        cluster.insert(
            "TerminationProtected".to_string(),
            json!(props
                .get("TerminationProtected")
                .and_then(Value::as_bool)
                .unwrap_or(false)),
        );
        cluster.insert(
            "VisibleToAllUsers".to_string(),
            json!(props
                .get("VisibleToAllUsers")
                .and_then(Value::as_bool)
                .unwrap_or(true)),
        );
        cluster.insert("NormalizedInstanceHours".to_string(), json!(0));
        cluster.insert(
            "StepConcurrencyLevel".to_string(),
            json!(props
                .get("StepConcurrencyLevel")
                .and_then(Value::as_i64)
                .unwrap_or(1)),
        );
        // Copy through the scalar/array members EMR echoes verbatim (the Cluster
        // shape is PascalCase, matching CFN).
        for key in [
            "ReleaseLabel",
            "LogUri",
            "LogEncryptionKmsKeyId",
            "ServiceRole",
            "AutoScalingRole",
            "ScaleDownBehavior",
            "CustomAmiId",
            "EbsRootVolumeSize",
            "SecurityConfiguration",
            "Applications",
            "Tags",
            "Configurations",
            "PlacementGroupConfigs",
            "EbsRootVolumeIops",
            "EbsRootVolumeThroughput",
        ] {
            if let Some(v) = props.get(key) {
                if !v.is_null() {
                    cluster.insert(key.to_string(), v.clone());
                }
            }
        }

        let mut guard = self.emr_state.write();
        let st = guard.get_or_create(account);
        st.clusters.insert(id.clone(), Value::Object(cluster));
        st.cluster_order.push(id.clone());

        Ok(ProvisionResult::new(id).with("MasterPublicDNS", master_dns))
    }

    pub(super) fn delete_emr_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.emr_state.write();
        let st = guard.get_or_create(&self.account_id);
        st.clusters.remove(physical_id);
        st.cluster_order.retain(|n| n != physical_id);
        Ok(())
    }

    pub(super) fn get_att_emr_cluster(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let guard = self.emr_state.read();
        let st = guard.get(&self.account_id)?;
        let cluster = st.clusters.get(physical_id)?;
        match attribute {
            "MasterPublicDNS" => cluster
                .get("MasterPublicDnsName")
                .and_then(Value::as_str)
                .map(str::to_string),
            _ => None,
        }
    }
}
