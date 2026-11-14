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

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};
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
            .unwrap_or_else(|| self.physical_name(resource));
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
        // VPCOptions resolve against EC2 (VPCId, AvailabilityZones, default
        // security group), exactly as the direct CreateDomain does.
        if let Some(v) = config.get("VPCOptions").filter(|v| v.is_object()).cloned() {
            config.insert(
                "VPCOptions".into(),
                fakecloud_opensearch::vpc::derive_vpc_options(
                    &self.ec2_state,
                    &self.account_id,
                    &self.region,
                    &v,
                )?,
            );
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

        let mut domain = Domain {
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
            service_software_status: None,
            last_change: None,
            last_dry_run: None,
            dry_runs: Vec::new(),
            data_plane: None,
            processing: None,
            data_plane_error: None,
            change_pending: false,
            replication: None,
        };
        // With a container runtime the domain is backed by a real search
        // cluster, brought up in the background after provisioning (the same
        // convergence the direct `CreateDomain` runs).
        let back_with_container = fakecloud_opensearch::backing::begin(
            self.opensearch_runtime.is_some(),
            &mut domain,
            fakecloud_opensearch::backing::CREATING,
        );

        let mut guard = self.opensearch_state.write();
        let st = guard.get_or_create(account);
        if st.domains.contains_key(&name) {
            return Err(format!("Domain {name} already exists"));
        }
        st.domains.insert(name.clone(), domain);
        drop(guard);
        if back_with_container {
            self.pending_container_spawns.lock().push(
                super::ContainerSpawnIntent::OpenSearchDomain {
                    domain_name: name.clone(),
                },
            );
        }

        Ok(ProvisionResult::new(name)
            .with("Arn", arn.clone())
            .with("DomainArn", arn)
            .with("Id", domain_id)
            .with("DomainEndpoint", endpoint.clone())
            .with("DomainEndpointV2", format!("{endpoint}.v2")))
    }

    pub(super) fn update_opensearch_domain(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.update_opensearch_domain_inner(existing, resource, false)
    }

    pub(super) fn update_elasticsearch_domain(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.update_opensearch_domain_inner(existing, resource, true)
    }

    /// In-place stack update for an OpenSearch / Elasticsearch domain. A domain
    /// is stateful (it holds indices/documents and can be container-backed), so
    /// a benign config or tag change must NOT delete+recreate it -- that would
    /// wipe every index. This mirrors `UpdateDomainConfig`: it re-projects the
    /// mutable configuration and tags from the new template onto the EXISTING
    /// stored domain, preserving its name, arn, endpoint, id, indices, data
    /// sources, and create time (the data survives).
    fn update_opensearch_domain_inner(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
        es: bool,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = existing.physical_id.clone();
        let engine_version = canonical_engine_version(props, es);

        let mut config: BTreeMap<String, Value> = BTreeMap::new();
        if let Some(obj) = props.as_object() {
            for (k, v) in obj {
                if matches!(k.as_str(), "DomainName" | "Tags" | "TagList") {
                    continue;
                }
                config.insert(k.clone(), v.clone());
            }
        }
        // VPCOptions resolve against EC2 (VPCId, AvailabilityZones, default
        // security group), exactly as the direct CreateDomain does.
        if let Some(v) = config.get("VPCOptions").filter(|v| v.is_object()).cloned() {
            config.insert(
                "VPCOptions".into(),
                fakecloud_opensearch::vpc::derive_vpc_options(
                    &self.ec2_state,
                    &self.account_id,
                    &self.region,
                    &v,
                )?,
            );
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

        let mut guard = self.opensearch_state.write();
        let st = guard.get_or_create(&self.account_id);
        let domain = st
            .domains
            .get_mut(&name)
            .ok_or_else(|| format!("Domain {name} not yet provisioned"))?;

        // Apply the new config/version/tags in place. Everything else on the
        // record -- indices, data_sources, endpoint, arn, created_at -- is left
        // untouched so the data behind the domain survives the update.
        domain.engine_version = engine_version;
        domain.config = config;
        domain.tags = tags;
        // A changed engine version or node settings replace the backing
        // cluster on the same data volume, as `UpdateDomainConfig` does.
        let restart = fakecloud_opensearch::backing::begin_change(
            self.opensearch_runtime.as_deref(),
            domain,
            fakecloud_opensearch::backing::MODIFYING,
        );

        let arn = domain.arn.clone();
        let domain_id = domain.domain_id.clone();
        let endpoint = domain.endpoint.clone();
        drop(guard);
        if restart {
            self.pending_container_spawns.lock().push(
                super::ContainerSpawnIntent::OpenSearchDomain {
                    domain_name: name.clone(),
                },
            );
        }

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
        let removed = st.domains.remove(physical_id);
        if let Some(d) = &removed {
            st.detach_replication_peer_of(&d.arn);
        }
        drop(guard);
        // Reap the backing cluster off the request path. Untracked now so a
        // same-named recreate in this stack op never reuses the container.
        if let (Some(rt), Some(d)) = (&self.opensearch_runtime, removed) {
            self.pending_container_teardowns.lock().push(
                super::ContainerTeardownIntent::OpenSearchDomain {
                    container_id: rt.untrack(&d.arn),
                    volume: fakecloud_opensearch::backing::domain_spec(&d).map(|s| s.volume),
                },
            );
        }
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
