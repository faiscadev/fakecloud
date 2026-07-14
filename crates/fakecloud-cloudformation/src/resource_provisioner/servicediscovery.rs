//! `AWS::ServiceDiscovery::*` (Cloud Map) CloudFormation provisioning. Builds
//! the servicediscovery crate's OWN state structs (namespaces, services,
//! instances) and inserts them into the shared Cloud Map state under the
//! provisioner's account, so a CFN-provisioned resource reads back through
//! `GetNamespace` / `GetService` / `ListInstances` exactly like an API-created
//! one. The Cloud Map service's snapshot hook (registered in the server) then
//! persists them so they survive a restart (#1766 lesson).

use std::collections::BTreeMap;

use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use fakecloud_servicediscovery::{
    DnsConfig, DnsProps, DnsRecord, HealthCheckConfig, HealthCheckCustomConfig, Instance,
    Namespace, Service,
};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner};

/// A lowercase alphanumeric fragment of the requested length, seeded from a
/// random uuid's hex (which is alphanumeric). Mirrors the shape the direct
/// Cloud Map service uses for `ns-`/`srv-` ids and hosted-zone ids.
fn frag(len: usize) -> String {
    let mut s = String::new();
    while s.len() < len {
        s.push_str(&Uuid::new_v4().simple().to_string());
    }
    s.truncate(len);
    s
}

fn new_namespace_id() -> String {
    format!("ns-{}", frag(17))
}

fn new_service_id() -> String {
    format!("srv-{}", frag(17))
}

fn new_hosted_zone_id() -> String {
    format!("Z{}", frag(21).to_uppercase())
}

fn namespace_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:servicediscovery:{region}:{account}:namespace/{id}")
}

fn service_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:servicediscovery:{region}:{account}:service/{id}")
}

/// Reduce a bare id or full ARN to its trailing resource id (Cloud Map accepts
/// either wherever a `NamespaceId` is expected).
fn resource_id_from(value: &str) -> &str {
    value.rsplit('/').next().unwrap_or(value)
}

impl ResourceProvisioner {
    // --- Namespaces ---

    fn create_sd_namespace(
        &self,
        resource: &ResourceDefinition,
        type_: &str,
        dns: Option<DnsProps>,
        vpc: Option<String>,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let id = new_namespace_id();
        let arn = namespace_arn(&self.region, &self.account_id, &id);
        let hosted_zone_id = dns.as_ref().map(|d| d.hosted_zone_id.clone());
        let ns = Namespace {
            id: id.clone(),
            arn: arn.clone(),
            name: name.clone(),
            type_: type_.to_string(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            service_count: 0,
            http_name: name,
            dns,
            vpc,
            creator_request_id: Uuid::new_v4().to_string(),
            create_date: Utc::now(),
        };
        let mut accounts = self.servicediscovery_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.namespaces.insert(id.clone(), ns);

        let mut result = ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("Id", id);
        if let Some(hz) = hosted_zone_id {
            result = result.with("HostedZoneId", hz);
        }
        Ok(result)
    }

    pub(super) fn create_sd_http_namespace(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        self.create_sd_namespace(resource, "HTTP", None, None)
    }

    pub(super) fn create_sd_public_dns_namespace(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let dns = DnsProps {
            hosted_zone_id: new_hosted_zone_id(),
            soa_ttl: sd_soa_ttl(&resource.properties),
        };
        self.create_sd_namespace(resource, "DNS_PUBLIC", Some(dns), None)
    }

    pub(super) fn create_sd_private_dns_namespace(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let vpc = resource
            .properties
            .get("Vpc")
            .and_then(|v| v.as_str())
            .ok_or("Vpc is required")?
            .to_string();
        let dns = DnsProps {
            hosted_zone_id: new_hosted_zone_id(),
            soa_ttl: sd_soa_ttl(&resource.properties),
        };
        self.create_sd_namespace(resource, "DNS_PRIVATE", Some(dns), Some(vpc))
    }

    pub(super) fn delete_sd_namespace(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.servicediscovery_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.namespaces.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_sd_namespace(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let accounts = self.servicediscovery_state.read();
        let ns = accounts
            .get(&self.account_id)?
            .namespaces
            .get(physical_id)?;
        match attribute {
            "Arn" => Some(ns.arn.clone()),
            "Id" => Some(ns.id.clone()),
            "HostedZoneId" => ns.dns.as_ref().map(|d| d.hosted_zone_id.clone()),
            _ => None,
        }
    }

    // --- Service ---

    pub(super) fn create_sd_service(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let namespace_id = props
            .get("NamespaceId")
            .and_then(|v| v.as_str())
            .or_else(|| {
                props
                    .get("DnsConfig")
                    .and_then(|d| d.get("NamespaceId"))
                    .and_then(|v| v.as_str())
            })
            .map(|s| resource_id_from(s).to_string())
            .ok_or("NamespaceId is required")?;
        let dns_config = props.get("DnsConfig").map(parse_dns_config);
        let health_check_config = props.get("HealthCheckConfig").map(parse_health_check);
        let health_check_custom_config = props
            .get("HealthCheckCustomConfig")
            .map(parse_health_check_custom);
        let explicit_http = props.get("Type").and_then(|v| v.as_str()) == Some("HTTP");

        let id = new_service_id();
        let arn = service_arn(&self.region, &self.account_id, &id);

        let mut accounts = self.servicediscovery_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let Some(ns) = state.namespaces.get(&namespace_id) else {
            return Err(format!("Namespace {namespace_id} not found."));
        };
        let type_ = if explicit_http || ns.type_ == "HTTP" {
            "HTTP".to_string()
        } else if dns_config.is_some() {
            "DNS_HTTP".to_string()
        } else {
            "HTTP".to_string()
        };
        let svc = Service {
            id: id.clone(),
            arn: arn.clone(),
            name: name.clone(),
            namespace_id: namespace_id.clone(),
            type_,
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            instance_count: 0,
            dns_config,
            health_check_config,
            health_check_custom_config,
            attributes: BTreeMap::new(),
            creator_request_id: Uuid::new_v4().to_string(),
            create_date: Utc::now(),
            instances: BTreeMap::new(),
            instances_revision: 0,
        };
        state.services.insert(id.clone(), svc);
        if let Some(ns) = state.namespaces.get_mut(&namespace_id) {
            ns.service_count += 1;
        }
        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name))
    }

    pub(super) fn delete_sd_service(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.servicediscovery_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(svc) = state.services.remove(physical_id) {
            // Keep the parent namespace's ServiceCount consistent.
            if let Some(ns) = state.namespaces.get_mut(&svc.namespace_id) {
                ns.service_count = (ns.service_count - 1).max(0);
            }
        }
        Ok(())
    }

    pub(super) fn get_att_sd_service(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let accounts = self.servicediscovery_state.read();
        let svc = accounts.get(&self.account_id)?.services.get(physical_id)?;
        match attribute {
            "Arn" => Some(svc.arn.clone()),
            "Id" => Some(svc.id.clone()),
            "Name" => Some(svc.name.clone()),
            _ => None,
        }
    }

    // --- Instance ---

    pub(super) fn create_sd_instance(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let service_id = props
            .get("ServiceId")
            .and_then(|v| v.as_str())
            .map(|s| resource_id_from(s).to_string())
            .ok_or("ServiceId is required")?;
        // CFN uses `InstanceId`; it defaults to the logical id when omitted.
        let instance_id = props
            .get("InstanceId")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let attributes: BTreeMap<String, String> = props
            .get("InstanceAttributes")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .ok_or("InstanceAttributes is required")?;

        let mut accounts = self.servicediscovery_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let Some(svc) = state.services.get_mut(&service_id) else {
            return Err(format!("Service {service_id} not found."));
        };
        let is_new = !svc.instances.contains_key(&instance_id);
        svc.instances.insert(
            instance_id.clone(),
            Instance {
                id: instance_id.clone(),
                creator_request_id: Uuid::new_v4().to_string(),
                attributes,
                health: "HEALTHY".to_string(),
            },
        );
        if is_new {
            svc.instance_count += 1;
        }
        svc.instances_revision += 1;
        // Ref is the InstanceId; the ServiceId is stashed in attributes so the
        // delete path (which only gets physical id + attributes) can locate it.
        Ok(ProvisionResult::new(instance_id).with("ServiceId", service_id))
    }

    pub(super) fn delete_sd_instance(
        &self,
        physical_id: &str,
        attrs: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let Some(service_id) = attrs.get("ServiceId") else {
            return Ok(());
        };
        let mut accounts = self.servicediscovery_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(svc) = state.services.get_mut(service_id) {
            if svc.instances.remove(physical_id).is_some() {
                svc.instance_count = (svc.instance_count - 1).max(0);
                svc.instances_revision += 1;
            }
        }
        Ok(())
    }
}

/// The SOA TTL from a public/private-DNS namespace's `Properties`, defaulting to
/// Cloud Map's 15s when absent.
fn sd_soa_ttl(props: &Value) -> i64 {
    props
        .get("Properties")
        .and_then(|p| p.get("DnsProperties"))
        .and_then(|d| d.get("SOA"))
        .and_then(|s| s.get("TTL"))
        .and_then(|v| v.as_i64())
        .unwrap_or(15)
}

fn parse_dns_records(v: &Value) -> Vec<DnsRecord> {
    v.as_array()
        .map(|arr| {
            arr.iter()
                .map(|r| DnsRecord {
                    type_: r
                        .get("Type")
                        .and_then(|x| x.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    ttl: r.get("TTL").and_then(|x| x.as_i64()).unwrap_or(60),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_dns_config(v: &Value) -> DnsConfig {
    DnsConfig {
        namespace_id: v
            .get("NamespaceId")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        routing_policy: v
            .get("RoutingPolicy")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        dns_records: v
            .get("DnsRecords")
            .map(parse_dns_records)
            .unwrap_or_default(),
    }
}

fn parse_health_check(v: &Value) -> HealthCheckConfig {
    HealthCheckConfig {
        type_: v
            .get("Type")
            .and_then(|x| x.as_str())
            .unwrap_or("HTTP")
            .to_string(),
        resource_path: v
            .get("ResourcePath")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        failure_threshold: v
            .get("FailureThreshold")
            .and_then(|x| x.as_i64())
            .map(|n| n as i32),
    }
}

fn parse_health_check_custom(v: &Value) -> HealthCheckCustomConfig {
    HealthCheckCustomConfig {
        failure_threshold: v
            .get("FailureThreshold")
            .and_then(|x| x.as_i64())
            .map(|n| n as i32),
    }
}
