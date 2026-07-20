//! `AWS::OpenSearchService::Domain` (and the legacy `AWS::Elasticsearch::Domain`)
//! CloudFormation provisioning. The domain is written through to the
//! `opensearch` service state as the same `Domain` record the direct
//! `CreateDomain` handler stores, so a CFN-created domain reads back identically
//! on `DescribeDomain` and persists through the `es` snapshot hook (survives a
//! restart -- #1766 class).
//!
//! `Ref` resolves to the domain name (the physical id). `Fn::GetAtt` exposes
//! `Arn`/`DomainArn`, `DomainEndpoint`, `Id`, and `DomainEndpointV2`.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};
use fakecloud_opensearch::state::{domain_arn, Domain};

impl ResourceProvisioner {
    pub(super) fn create_opensearch_domain(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.create_opensearch_domain_inner(resource, false)
    }

    pub(super) fn create_elasticsearch_domain(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.create_opensearch_domain_inner(resource, true)
    }

    fn create_opensearch_domain_inner(
        &self,
        resource: &ResourceDefinition,
        es: bool,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("DomainName")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| resource.logical_id.clone());
        let region = &self.region;
        let account = &self.account_id;
        let arn = domain_arn(region, account, &name);
        let domain_id = format!("{account}/{name}");
        let short = uuid::Uuid::new_v4().simple().to_string()[..10].to_string();
        let endpoint = format!("search-{name}-{short}.{region}.es.amazonaws.com");
        let engine_version = canonical_engine_version(props, es);

        // Every create-input member except DomainName / TagList is stored
        // verbatim in `config` (the ES API is PascalCase, matching CFN), so
        // DescribeDomain projects ClusterConfig / EBSOptions / VPCOptions / ...
        // back exactly as written.
        let mut config: BTreeMap<String, Value> = BTreeMap::new();
        if let Some(obj) = props.as_object() {
            for (k, v) in obj {
                if matches!(k.as_str(), "DomainName" | "Tags" | "TagList") {
                    continue;
                }
                config.insert(k.clone(), v.clone());
            }
        }

        let mut tags: BTreeMap<String, String> = BTreeMap::new();
        if let Some(arr) = props.get("Tags").and_then(Value::as_array) {
            for t in arr {
                if let (Some(k), Some(val)) = (
                    t.get("Key").and_then(Value::as_str),
                    t.get("Value").and_then(Value::as_str),
                ) {
                    tags.insert(k.to_string(), val.to_string());
                }
            }
        }

        let domain = Domain {
            name: name.clone(),
            domain_id: domain_id.clone(),
            arn: arn.clone(),
            engine_version,
            created_via_es: es,
            endpoint: endpoint.clone(),
            created: false,
            deleted: false,
            config,
            tags,
            created_at: chrono::Utc::now(),
            data_sources: Default::default(),
            indices: Default::default(),
            scheduled_actions: Default::default(),
            maintenances: Default::default(),
        };

        let mut guard = self.opensearch_state.write();
        let st = guard.get_or_create(account);
        if st.domains.contains_key(&name) {
            return Err(format!("Domain {name} already exists"));
        }
        st.domains.insert(name.clone(), domain);

        Ok(ProvisionResult::new(name)
            .with("Arn", arn.clone())
            .with("DomainArn", arn)
            .with("Id", domain_id)
            .with("DomainEndpoint", endpoint.clone())
            .with("DomainEndpointV2", format!("{endpoint}.v2")))
    }

    pub(super) fn delete_opensearch_domain(&self, physical_id: &str) -> Result<(), String> {
        let mut guard = self.opensearch_state.write();
        let st = guard.get_or_create(&self.account_id);
        st.domains.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_opensearch_domain(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let guard = self.opensearch_state.read();
        let st = guard.get(&self.account_id)?;
        let d = st.domains.get(physical_id)?;
        match attribute {
            "Arn" | "DomainArn" => Some(d.arn.clone()),
            "Id" => Some(d.domain_id.clone()),
            "DomainEndpoint" => Some(d.endpoint.clone()),
            "DomainEndpointV2" => Some(format!("{}.v2", d.endpoint)),
            _ => None,
        }
    }
}

/// Canonicalize the engine version to the prefixed form the service stores
/// (`OpenSearch_2.11` / `Elasticsearch_7.10`), defaulting when the template
/// omits it.
fn canonical_engine_version(props: &Value, es: bool) -> String {
    let raw = props
        .get("EngineVersion")
        .or_else(|| props.get("ElasticsearchVersion"))
        .and_then(Value::as_str);
    match raw {
        Some(v) if v.contains('_') => v.to_string(),
        Some(v) if es => format!("Elasticsearch_{v}"),
        Some(v) => format!("OpenSearch_{v}"),
        None if es => "Elasticsearch_7.10".to_string(),
        None => "OpenSearch_2.11".to_string(),
    }
}
