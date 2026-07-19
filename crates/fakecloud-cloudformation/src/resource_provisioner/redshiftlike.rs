//! CloudFormation provisioning for the RDS-shaped cluster services Redshift,
//! DocDB, and Neptune. Each arm routes through the owning service's REAL create/
//! delete handler (`provision_sync`) so a CFN-provisioned cluster is created with
//! identical state and validation to the direct API — not recorded as a phantom
//! resource by the `create_resource` catch-all (which gave `Ref` the bare
//! logical id and failed `GetAtt` on real attributes). Persistence is handled by
//! the CloudFormation persist layer, which fires each service's registered
//! snapshot hook after the stack op (see `service_key_for_type`).

use std::collections::HashMap;

use bytes::Bytes;
use fakecloud_core::service::AwsRequest;
use http::{HeaderMap, Method};
use serde_json::Value;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};

/// Build a Query-protocol `AwsRequest` for a control-plane action.
fn cluster_request(
    provisioner: &ResourceProvisioner,
    service: &str,
    action: &str,
    params: HashMap<String, String>,
) -> AwsRequest {
    AwsRequest {
        service: service.to_string(),
        action: action.to_string(),
        region: provisioner.region.clone(),
        account_id: provisioner.account_id.clone(),
        request_id: "cfn".to_string(),
        headers: HeaderMap::new(),
        query_params: params,
        body: Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

/// Flatten a CloudFormation `properties` object into Query-protocol params.
/// Scalars map by name (CFN property names match the Query param names for
/// these services); string arrays become `Key.member.N` (the member-list wire
/// form the handlers read).
fn props_to_query(props: &Value) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let Some(obj) = props.as_object() else {
        return params;
    };
    for (k, v) in obj {
        match v {
            Value::String(s) => {
                params.insert(k.clone(), s.clone());
            }
            Value::Number(n) => {
                params.insert(k.clone(), n.to_string());
            }
            Value::Bool(b) => {
                params.insert(k.clone(), b.to_string());
            }
            Value::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    if let Some(s) = item.as_str() {
                        params.insert(format!("{k}.member.{}", i + 1), s.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    params
}

/// Derive a stable cluster identifier from the template (or synthesize one when
/// the property is omitted, as CloudFormation does).
fn cluster_identifier(props: &Value, key: &str, logical_id: &str) -> String {
    props
        .get(key)
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| {
            format!(
                "cfn-{}-{}",
                logical_id.to_lowercase(),
                fakecloud_core::ids::short_id(8).to_lowercase()
            )
        })
}

impl ResourceProvisioner {
    // --- AWS::Redshift::Cluster ---

    pub(super) fn create_redshift_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = cluster_identifier(props, "ClusterIdentifier", &resource.logical_id);
        let mut params = props_to_query(props);
        params.insert("ClusterIdentifier".to_string(), id.clone());

        let svc = fakecloud_redshift::RedshiftService::new(self.redshift_state.clone());
        let req = cluster_request(self, "redshift", "CreateCluster", params);
        svc.provision_sync(&req)
            .map_err(|e| format!("Redshift CreateCluster failed: {}", e.message()))?;

        // Read the created cluster back for the Ref/GetAtt attributes.
        let (endpoint, port) = {
            let mut guard = self.redshift_state.write();
            let acct = guard.account(&self.account_id);
            match acct.clusters.get(&id) {
                Some(c) => (c.endpoint_address.clone(), c.endpoint_port),
                None => (String::new(), 5439),
            }
        };
        Ok(ProvisionResult::new(id.clone())
            .with("Id", id)
            .with("Endpoint.Address", endpoint)
            .with("Endpoint.Port", port.to_string()))
    }

    pub(super) fn delete_redshift_cluster(&self, physical_id: &str) -> Result<(), String> {
        let svc = fakecloud_redshift::RedshiftService::new(self.redshift_state.clone());
        let mut params = HashMap::new();
        params.insert("ClusterIdentifier".to_string(), physical_id.to_string());
        params.insert("SkipFinalClusterSnapshot".to_string(), "true".to_string());
        let req = cluster_request(self, "redshift", "DeleteCluster", params);
        // A delete of an already-gone cluster is not a stack failure.
        let _ = svc.provision_sync(&req);
        Ok(())
    }

    pub(super) fn get_att_redshift_cluster(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut guard = self.redshift_state.write();
        let acct = guard.account(&self.account_id);
        let c = acct.clusters.get(physical_id)?;
        match attribute {
            "Id" => Some(c.cluster_identifier.clone()),
            "Endpoint.Address" => Some(c.endpoint_address.clone()),
            "Endpoint.Port" => Some(c.endpoint_port.to_string()),
            _ => None,
        }
    }

    // --- AWS::DocDB::DBCluster ---

    pub(super) fn create_docdb_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = cluster_identifier(props, "DBClusterIdentifier", &resource.logical_id);
        let mut params = props_to_query(props);
        params.insert("DBClusterIdentifier".to_string(), id.clone());
        // `AWS::DocDB::DBCluster` has no Engine property (always docdb); the
        // handler requires one.
        params
            .entry("Engine".to_string())
            .or_insert_with(|| "docdb".to_string());

        let svc = fakecloud_docdb::DocDbService::new(self.docdb_state.clone());
        let req = cluster_request(self, "docdb", "CreateDBCluster", params);
        svc.provision_sync(&req)
            .map_err(|e| format!("DocDB CreateDBCluster failed: {}", e.message()))?;

        let result = {
            let mut guard = self.docdb_state.write();
            let st = guard.get_or_create(&self.account_id);
            let mut r = ProvisionResult::new(id.clone());
            if let Some(c) = st.clusters.get(&id) {
                r = r
                    .with("Endpoint", c.endpoint.clone())
                    .with("ReadEndpoint", c.reader_endpoint.clone())
                    .with("Port", c.port.to_string())
                    .with("ClusterResourceId", c.db_cluster_resource_id.clone());
            }
            r
        };
        Ok(result)
    }

    pub(super) fn delete_docdb_cluster(&self, physical_id: &str) -> Result<(), String> {
        let svc = fakecloud_docdb::DocDbService::new(self.docdb_state.clone());
        let mut params = HashMap::new();
        params.insert("DBClusterIdentifier".to_string(), physical_id.to_string());
        params.insert("SkipFinalSnapshot".to_string(), "true".to_string());
        let req = cluster_request(self, "docdb", "DeleteDBCluster", params);
        let _ = svc.provision_sync(&req);
        Ok(())
    }

    pub(super) fn get_att_docdb_cluster(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut guard = self.docdb_state.write();
        let st = guard.get_or_create(&self.account_id);
        let c = st.clusters.get(physical_id)?;
        match attribute {
            "Endpoint" => Some(c.endpoint.clone()),
            "ReadEndpoint" => Some(c.reader_endpoint.clone()),
            "Port" => Some(c.port.to_string()),
            "ClusterResourceId" => Some(c.db_cluster_resource_id.clone()),
            _ => None,
        }
    }

    // --- AWS::Neptune::DBCluster ---

    pub(super) fn create_neptune_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = cluster_identifier(props, "DBClusterIdentifier", &resource.logical_id);
        let mut params = props_to_query(props);
        params.insert("DBClusterIdentifier".to_string(), id.clone());
        // `AWS::Neptune::DBCluster` has no Engine property (always neptune); the
        // handler requires one.
        params
            .entry("Engine".to_string())
            .or_insert_with(|| "neptune".to_string());

        let svc = fakecloud_neptune::NeptuneService::new(self.neptune_state.clone());
        let req = cluster_request(self, "neptune", "CreateDBCluster", params);
        svc.provision_sync(&req)
            .map_err(|e| format!("Neptune CreateDBCluster failed: {}", e.message()))?;

        let result = {
            let mut guard = self.neptune_state.write();
            let st = guard.get_or_create(&self.account_id);
            let mut r = ProvisionResult::new(id.clone());
            if let Some(c) = st.clusters.get(&id) {
                r = r
                    .with("Endpoint", c.endpoint.clone())
                    .with("ReadEndpoint", c.reader_endpoint.clone())
                    .with("Port", c.port.to_string())
                    .with("ClusterResourceId", c.db_cluster_resource_id.clone());
            }
            r
        };
        Ok(result)
    }

    pub(super) fn delete_neptune_cluster(&self, physical_id: &str) -> Result<(), String> {
        let svc = fakecloud_neptune::NeptuneService::new(self.neptune_state.clone());
        let mut params = HashMap::new();
        params.insert("DBClusterIdentifier".to_string(), physical_id.to_string());
        params.insert("SkipFinalSnapshot".to_string(), "true".to_string());
        let req = cluster_request(self, "neptune", "DeleteDBCluster", params);
        let _ = svc.provision_sync(&req);
        Ok(())
    }

    pub(super) fn get_att_neptune_cluster(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut guard = self.neptune_state.write();
        let st = guard.get_or_create(&self.account_id);
        let c = st.clusters.get(physical_id)?;
        match attribute {
            "Endpoint" => Some(c.endpoint.clone()),
            "ReadEndpoint" => Some(c.reader_endpoint.clone()),
            "Port" => Some(c.port.to_string()),
            "ClusterResourceId" => Some(c.db_cluster_resource_id.clone()),
            _ => None,
        }
    }
}
