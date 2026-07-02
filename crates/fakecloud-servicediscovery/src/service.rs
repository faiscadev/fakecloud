//! Cloud Map (`servicediscovery`) awsJson1.1 dispatch + operation handlers.
//!
//! This batch implements the namespace control plane plus Cloud Map's
//! asynchronous operation-tracking primitive. Cloud Map's create/update/delete
//! calls are *asynchronous*: they don't return the resource, they return an
//! `OperationId`. The caller then polls `GetOperation` until the operation
//! reports `SUCCESS`. We model that faithfully — a create/update/delete mints
//! an [`Operation`] in `SUBMITTED` state, applies the state change eagerly, and
//! settles the operation to `SUCCESS` deterministically on the first
//! `GetOperation` read (the same lazy-settle pattern EKS uses for
//! `CREATING -> ACTIVE`). No background timer is needed for the control-plane
//! contract, and the settled state is snapshotted on the next mutating call.
//!
//! Services, instances, and tag operations register *into* these namespaces and
//! land in a later batch; dispatch already routes their names to
//! `action_not_implemented` so they surface cleanly as unimplemented.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{
    DnsConfig, DnsProps, DnsRecord, HealthCheckConfig, HealthCheckCustomConfig, Instance,
    Namespace, Operation, Service, SharedServiceDiscoveryState,
};

/// Every operation name in the Cloud Map Smithy model (all 30). Ops not yet
/// implemented in this batch (services/instances/tags) still advertise here so
/// the router recognises them and returns a clean "not implemented" rather than
/// an unknown-action 404; they are wired in later batches.
pub const SERVICEDISCOVERY_ACTIONS: &[&str] = &[
    // Namespaces + operations (this batch).
    "CreateHttpNamespace",
    "CreatePrivateDnsNamespace",
    "CreatePublicDnsNamespace",
    "DeleteNamespace",
    "GetNamespace",
    "GetOperation",
    "ListNamespaces",
    "ListOperations",
    "UpdateHttpNamespace",
    "UpdatePrivateDnsNamespace",
    "UpdatePublicDnsNamespace",
    // Services (later batch).
    "CreateService",
    "DeleteService",
    "GetService",
    "ListServices",
    "UpdateService",
    "DeleteServiceAttributes",
    "GetServiceAttributes",
    "UpdateServiceAttributes",
    // Instances (later batch).
    "DeregisterInstance",
    "DiscoverInstances",
    "DiscoverInstancesRevision",
    "GetInstance",
    "GetInstancesHealthStatus",
    "ListInstances",
    "RegisterInstance",
    "UpdateInstanceCustomHealthStatus",
    // Tags (later batch).
    "ListTagsForResource",
    "TagResource",
    "UntagResource",
];

/// The default SOA record TTL Cloud Map applies to a DNS namespace when the
/// caller doesn't specify one.
const DEFAULT_SOA_TTL: i64 = 15;

pub struct ServiceDiscoveryService {
    state: SharedServiceDiscoveryState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl ServiceDiscoveryService {
    pub fn new(state: SharedServiceDiscoveryState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    /// Lets a CFN-provisioned Cloud Map namespace/service/instance survive a
    /// restart, matching the direct-API persistence path (#1766 class).
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

#[async_trait]
impl AwsService for ServiceDiscoveryService {
    fn service_name(&self) -> &str {
        "servicediscovery"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = dispatch(self, &request);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SERVICEDISCOVERY_ACTIONS
    }
}

/// GetOperation settles an operation to SUCCESS on read, mutating state on an
/// otherwise read-only path. That transition is idempotent and self-healing
/// (re-applied on the next read after a restart), so we snapshot it here to
/// persist the settled status without waiting for the next create/delete.
fn is_mutating(action: &str) -> bool {
    action.starts_with("Create")
        || action.starts_with("Delete")
        || action.starts_with("Update")
        || action.starts_with("Register")
        || action.starts_with("Deregister")
        || action.starts_with("Tag")
        || action.starts_with("Untag")
        || action == "GetOperation"
}

fn dispatch(s: &ServiceDiscoveryService, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
    match req.action.as_str() {
        "CreateHttpNamespace" => s.create_http_namespace(req),
        "CreatePrivateDnsNamespace" => s.create_private_dns_namespace(req),
        "CreatePublicDnsNamespace" => s.create_public_dns_namespace(req),
        "DeleteNamespace" => s.delete_namespace(req),
        "GetNamespace" => s.get_namespace(req),
        "GetOperation" => s.get_operation(req),
        "ListNamespaces" => s.list_namespaces(req),
        "ListOperations" => s.list_operations(req),
        "UpdateHttpNamespace" => s.update_http_namespace(req),
        "UpdatePrivateDnsNamespace" => s.update_private_dns_namespace(req),
        "UpdatePublicDnsNamespace" => s.update_public_dns_namespace(req),
        "CreateService" => s.create_service(req),
        "GetService" => s.get_service(req),
        "ListServices" => s.list_services(req),
        "UpdateService" => s.update_service(req),
        "DeleteService" => s.delete_service(req),
        "GetServiceAttributes" => s.get_service_attributes(req),
        "UpdateServiceAttributes" => s.update_service_attributes(req),
        "DeleteServiceAttributes" => s.delete_service_attributes(req),
        "RegisterInstance" => s.register_instance(req),
        "DeregisterInstance" => s.deregister_instance(req),
        "GetInstance" => s.get_instance(req),
        "ListInstances" => s.list_instances(req),
        "GetInstancesHealthStatus" => s.get_instances_health_status(req),
        "UpdateInstanceCustomHealthStatus" => s.update_instance_custom_health_status(req),
        "DiscoverInstances" => s.discover_instances(req),
        "DiscoverInstancesRevision" => s.discover_instances_revision(req),
        "TagResource" => s.tag_resource(req),
        "UntagResource" => s.untag_resource(req),
        "ListTagsForResource" => s.list_tags_for_resource(req),
        _ => Err(AwsServiceError::action_not_implemented(
            s.service_name(),
            &req.action,
        )),
    }
}

// ===== helpers =====

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn parse(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| fault("InvalidInput", &format!("malformed body: {e}")))
}

/// Map a Cloud Map error code to its HTTP status per the Smithy model:
/// `NamespaceNotFound`/`OperationNotFound` are 404, `DuplicateRequest`/
/// `ResourceInUse` are 409, everything else (`InvalidInput`,
/// `NamespaceAlreadyExists`, `ResourceLimitExceeded`) is 400.
fn fault(code: &str, msg: &str) -> AwsServiceError {
    let status = match code {
        "NamespaceNotFound"
        | "OperationNotFound"
        | "ServiceNotFound"
        | "InstanceNotFound"
        | "CustomHealthNotFound"
        | "ResourceNotFoundException" => StatusCode::NOT_FOUND,
        "DuplicateRequest" | "ResourceInUse" => StatusCode::CONFLICT,
        _ => StatusCode::BAD_REQUEST,
    };
    AwsServiceError::aws_error(status, code, msg)
}

fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .ok_or_else(|| fault("InvalidInput", &format!("{f} is required.")))
}

fn opt_str(b: &Value, f: &str) -> Option<String> {
    b.get(f).and_then(Value::as_str).map(str::to_string)
}

/// Lowercase alphanumeric id fragment derived from a fresh UUID.
fn rand_frag(len: usize) -> String {
    Uuid::new_v4().simple().to_string()[..len].to_string()
}

/// `ns-...` namespace id (AWS uses a lowercase alphanumeric suffix).
fn new_namespace_id() -> String {
    format!("ns-{}", rand_frag(17))
}

/// Cloud Map operation ids look like `<32 chars>-<8 chars>`.
fn new_operation_id() -> String {
    format!("{}-{}", rand_frag(32), rand_frag(8))
}

/// `Z...` hosted zone id for a public/private DNS namespace.
fn new_hosted_zone_id() -> String {
    format!("Z{}", rand_frag(21).to_uppercase())
}

fn namespace_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:servicediscovery:{region}:{account}:namespace/{id}")
}

/// `srv-...` service id (AWS uses a lowercase alphanumeric suffix).
fn new_service_id() -> String {
    format!("srv-{}", rand_frag(17))
}

fn service_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:servicediscovery:{region}:{account}:service/{id}")
}

/// Cloud Map accepts either a bare resource id (`ns-...`/`srv-...`) or a full
/// ARN wherever an id is expected (shared-namespace support). Reduce an ARN to
/// its trailing resource id so lookups key uniformly on the id.
fn resource_id_from(value: &str) -> &str {
    value.rsplit('/').next().unwrap_or(value)
}

fn creator_request_id(b: &Value) -> String {
    opt_str(b, "CreatorRequestId").unwrap_or_else(|| Uuid::new_v4().to_string())
}

/// Enforce a `smithy.api#length` max-bound on a string field (by Unicode
/// scalar count, matching Smithy length semantics), raising `InvalidInput` when
/// exceeded — the constraint validation Cloud Map performs on the wire.
fn check_max_len(value: &str, max: usize, field: &str) -> Result<(), AwsServiceError> {
    if value.chars().count() > max {
        return Err(fault(
            "InvalidInput",
            &format!("{field} exceeds the maximum length of {max}."),
        ));
    }
    Ok(())
}

/// Validate the shared `MaxResults` (range 1..=100) and `NextToken`
/// (max length 4096) pagination inputs, per the Smithy constraints.
fn validate_pagination(b: &Value) -> Result<(), AwsServiceError> {
    if let Some(mr) = b.get("MaxResults").and_then(Value::as_i64) {
        if !(1..=100).contains(&mr) {
            return Err(fault(
                "InvalidInput",
                "MaxResults must be between 1 and 100.",
            ));
        }
    }
    if let Some(tok) = b.get("NextToken").and_then(Value::as_str) {
        check_max_len(tok, 4096, "NextToken")?;
    }
    Ok(())
}

/// Read `Properties.DnsProperties.SOA.TTL` from a create request, defaulting
/// to [`DEFAULT_SOA_TTL`] when the optional `Properties` block is absent.
fn soa_ttl_from_create(b: &Value) -> i64 {
    b.get("Properties")
        .and_then(|p| p.get("DnsProperties"))
        .and_then(|d| d.get("SOA"))
        .and_then(|s| s.get("TTL"))
        .and_then(Value::as_i64)
        .unwrap_or(DEFAULT_SOA_TTL)
}

// ===== pagination =====

fn page_start(b: &Value) -> Option<usize> {
    match b.get("NextToken").and_then(Value::as_str) {
        None | Some("") => Some(0),
        Some(t) => t.parse::<usize>().ok(),
    }
}

fn max_results(b: &Value, default: usize) -> usize {
    b.get("MaxResults")
        .and_then(Value::as_i64)
        .map(|n| n.clamp(1, 100) as usize)
        .unwrap_or(default)
}

fn paginate(items: Vec<Value>, b: &Value, default_max: usize) -> (Vec<Value>, Option<String>) {
    let Some(start) = page_start(b) else {
        return (Vec::new(), None);
    };
    let max = max_results(b, default_max);
    let total = items.len();
    if start >= total {
        return (Vec::new(), None);
    }
    let end = start.saturating_add(max).min(total);
    let next = (end < total).then(|| end.to_string());
    (items[start..end].to_vec(), next)
}

// ===== JSON builders (member names match the Smithy model) =====

fn namespace_properties_json(ns: &Namespace) -> Value {
    let mut props = json!({
        "HttpProperties": { "HttpName": ns.http_name },
    });
    if let Some(dns) = &ns.dns {
        props["DnsProperties"] = json!({
            "HostedZoneId": dns.hosted_zone_id,
            "SOA": { "TTL": dns.soa_ttl },
        });
    }
    props
}

/// Insert `key -> value` into an object only when `value` is `Some`, so absent
/// optional members are omitted from the wire (awsJson1.1 does not serialize
/// null for absent optionals).
fn put_opt(obj: &mut serde_json::Map<String, Value>, key: &str, value: Option<&String>) {
    if let Some(v) = value {
        obj.insert(key.to_string(), Value::String(v.clone()));
    }
}

/// Full `Namespace` shape (GetNamespace).
fn namespace_json(ns: &Namespace, account: &str) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("Id".into(), json!(ns.id));
    obj.insert("Arn".into(), json!(ns.arn));
    obj.insert("ResourceOwner".into(), json!(account));
    obj.insert("Name".into(), json!(ns.name));
    obj.insert("Type".into(), json!(ns.type_));
    put_opt(&mut obj, "Description", ns.description.as_ref());
    obj.insert("ServiceCount".into(), json!(ns.service_count));
    obj.insert("Properties".into(), namespace_properties_json(ns));
    obj.insert("CreateDate".into(), json!(ns.create_date.timestamp()));
    obj.insert("CreatorRequestId".into(), json!(ns.creator_request_id));
    Value::Object(obj)
}

/// `NamespaceSummary` shape (ListNamespaces) — same fields minus
/// `CreatorRequestId`.
fn namespace_summary_json(ns: &Namespace, account: &str) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("Id".into(), json!(ns.id));
    obj.insert("Arn".into(), json!(ns.arn));
    obj.insert("ResourceOwner".into(), json!(account));
    obj.insert("Name".into(), json!(ns.name));
    obj.insert("Type".into(), json!(ns.type_));
    put_opt(&mut obj, "Description", ns.description.as_ref());
    obj.insert("ServiceCount".into(), json!(ns.service_count));
    obj.insert("Properties".into(), namespace_properties_json(ns));
    obj.insert("CreateDate".into(), json!(ns.create_date.timestamp()));
    Value::Object(obj)
}

fn operation_json(op: &Operation, account: &str) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("Id".into(), json!(op.id));
    obj.insert("OwnerAccount".into(), json!(account));
    obj.insert("Type".into(), json!(op.type_));
    obj.insert("Status".into(), json!(op.status));
    put_opt(&mut obj, "ErrorMessage", op.error_message.as_ref());
    put_opt(&mut obj, "ErrorCode", op.error_code.as_ref());
    obj.insert("CreateDate".into(), json!(op.create_date.timestamp()));
    obj.insert("UpdateDate".into(), json!(op.update_date.timestamp()));
    obj.insert("Targets".into(), json!(op.targets));
    Value::Object(obj)
}

// ===== service parsing =====

/// Parse a `DnsRecord` list from a request `DnsRecords` array. Each record
/// requires a `Type` and `TTL` per the Smithy model.
fn parse_dns_records(v: &Value) -> Result<Vec<DnsRecord>, AwsServiceError> {
    let arr = v
        .as_array()
        .ok_or_else(|| fault("InvalidInput", "DnsRecords must be a list."))?;
    let mut out = Vec::with_capacity(arr.len());
    for r in arr {
        let type_ = req_str(r, "Type")?.to_string();
        let ttl = r
            .get("TTL")
            .and_then(Value::as_i64)
            .ok_or_else(|| fault("InvalidInput", "DnsRecord TTL is required."))?;
        out.push(DnsRecord { type_, ttl });
    }
    Ok(out)
}

/// Parse the `DnsConfig` block from a `CreateService` request.
fn parse_dns_config(v: &Value) -> Result<DnsConfig, AwsServiceError> {
    let records = v
        .get("DnsRecords")
        .ok_or_else(|| fault("InvalidInput", "DnsConfig.DnsRecords is required."))?;
    Ok(DnsConfig {
        namespace_id: opt_str(v, "NamespaceId"),
        routing_policy: opt_str(v, "RoutingPolicy"),
        dns_records: parse_dns_records(records)?,
    })
}

/// Parse a `HealthCheckConfig` block.
fn parse_health_check(v: &Value) -> Result<HealthCheckConfig, AwsServiceError> {
    Ok(HealthCheckConfig {
        type_: req_str(v, "Type")?.to_string(),
        resource_path: opt_str(v, "ResourcePath"),
        failure_threshold: v
            .get("FailureThreshold")
            .and_then(Value::as_i64)
            .map(|n| n as i32),
    })
}

/// Parse a `HealthCheckCustomConfig` block.
fn parse_health_check_custom(v: &Value) -> HealthCheckCustomConfig {
    HealthCheckCustomConfig {
        failure_threshold: v
            .get("FailureThreshold")
            .and_then(Value::as_i64)
            .map(|n| n as i32),
    }
}

// ===== service JSON builders (member names match the Smithy model) =====

fn dns_config_json(cfg: &DnsConfig) -> Value {
    let records: Vec<Value> = cfg
        .dns_records
        .iter()
        .map(|r| json!({ "Type": r.type_, "TTL": r.ttl }))
        .collect();
    let mut obj = serde_json::Map::new();
    put_opt(&mut obj, "NamespaceId", cfg.namespace_id.as_ref());
    put_opt(&mut obj, "RoutingPolicy", cfg.routing_policy.as_ref());
    obj.insert("DnsRecords".into(), Value::Array(records));
    Value::Object(obj)
}

fn health_check_json(hc: &HealthCheckConfig) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("Type".into(), json!(hc.type_));
    put_opt(&mut obj, "ResourcePath", hc.resource_path.as_ref());
    if let Some(ft) = hc.failure_threshold {
        obj.insert("FailureThreshold".into(), json!(ft));
    }
    Value::Object(obj)
}

fn health_check_custom_json(hc: &HealthCheckCustomConfig) -> Value {
    let mut obj = serde_json::Map::new();
    if let Some(ft) = hc.failure_threshold {
        obj.insert("FailureThreshold".into(), json!(ft));
    }
    Value::Object(obj)
}

/// Populate the members shared by the full `Service` shape and `ServiceSummary`.
fn service_common_members(obj: &mut serde_json::Map<String, Value>, svc: &Service, account: &str) {
    obj.insert("Id".into(), json!(svc.id));
    obj.insert("Arn".into(), json!(svc.arn));
    obj.insert("ResourceOwner".into(), json!(account));
    obj.insert("Name".into(), json!(svc.name));
    obj.insert("Type".into(), json!(svc.type_));
    put_opt(obj, "Description", svc.description.as_ref());
    obj.insert("InstanceCount".into(), json!(svc.instance_count));
    if let Some(dns) = &svc.dns_config {
        obj.insert("DnsConfig".into(), dns_config_json(dns));
    }
    if let Some(hc) = &svc.health_check_config {
        obj.insert("HealthCheckConfig".into(), health_check_json(hc));
    }
    if let Some(hc) = &svc.health_check_custom_config {
        obj.insert(
            "HealthCheckCustomConfig".into(),
            health_check_custom_json(hc),
        );
    }
    obj.insert("CreateDate".into(), json!(svc.create_date.timestamp()));
    obj.insert("CreatedByAccount".into(), json!(account));
}

/// Full `Service` shape (CreateService/GetService).
fn service_json(svc: &Service, account: &str) -> Value {
    let mut obj = serde_json::Map::new();
    service_common_members(&mut obj, svc, account);
    obj.insert("NamespaceId".into(), json!(svc.namespace_id));
    obj.insert("CreatorRequestId".into(), json!(svc.creator_request_id));
    Value::Object(obj)
}

/// `ServiceSummary` shape (ListServices) — no `NamespaceId`/`CreatorRequestId`.
fn service_summary_json(svc: &Service, account: &str) -> Value {
    let mut obj = serde_json::Map::new();
    service_common_members(&mut obj, svc, account);
    Value::Object(obj)
}

fn service_attributes_json(svc: &Service, account: &str) -> Value {
    json!({
        "ServiceArn": svc.arn,
        "ResourceOwner": account,
        "Attributes": svc.attributes,
    })
}

// ===== instance parsing + JSON builders =====

/// Parse the required `Attributes` map from a request into an ordered map.
/// Every value must be a string per the `Attributes` map shape.
fn parse_attributes(v: &Value) -> Result<BTreeMap<String, String>, AwsServiceError> {
    let obj = v
        .as_object()
        .ok_or_else(|| fault("InvalidInput", "Attributes must be a map."))?;
    let mut out = BTreeMap::new();
    for (k, val) in obj {
        let s = val
            .as_str()
            .ok_or_else(|| fault("InvalidInput", "Attribute values must be strings."))?;
        out.insert(k.clone(), s.to_string());
    }
    Ok(out)
}

/// Validate a registering instance's attributes against the service's routing
/// config: a DNS service materializes a record per `DnsRecord` type, and each
/// record type requires the matching well-known attribute (AWS raises
/// `InvalidInput` otherwise). HTTP-only services impose no attribute rule.
fn validate_instance_attributes(
    svc: &Service,
    attrs: &BTreeMap<String, String>,
) -> Result<(), AwsServiceError> {
    let Some(dns) = &svc.dns_config else {
        return Ok(());
    };
    for rec in &dns.dns_records {
        let (required, why) = match rec.type_.as_str() {
            "A" => ("AWS_INSTANCE_IPV4", "an A record"),
            "AAAA" => ("AWS_INSTANCE_IPV6", "an AAAA record"),
            "SRV" => ("AWS_INSTANCE_PORT", "an SRV record"),
            "CNAME" => ("AWS_INSTANCE_CNAME", "a CNAME record"),
            _ => continue,
        };
        // An AWS_ALIAS_DNS_NAME registration targets an ELB alias and is exempt
        // from the per-record-type attribute requirement.
        if attrs.contains_key("AWS_ALIAS_DNS_NAME") {
            continue;
        }
        if !attrs.contains_key(required) {
            return Err(fault(
                "InvalidInput",
                &format!("The service specifies {why}, so {required} is required."),
            ));
        }
    }
    Ok(())
}

fn instance_json(inst: &Instance, account: &str) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("Id".into(), json!(inst.id));
    obj.insert("CreatorRequestId".into(), json!(inst.creator_request_id));
    obj.insert("Attributes".into(), json!(inst.attributes));
    obj.insert("CreatedByAccount".into(), json!(account));
    Value::Object(obj)
}

fn instance_summary_json(inst: &Instance, account: &str) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("Id".into(), json!(inst.id));
    obj.insert("Attributes".into(), json!(inst.attributes));
    obj.insert("CreatedByAccount".into(), json!(account));
    Value::Object(obj)
}

fn http_instance_summary_json(inst: &Instance, namespace_name: &str, service_name: &str) -> Value {
    json!({
        "InstanceId": inst.id,
        "NamespaceName": namespace_name,
        "ServiceName": service_name,
        "HealthStatus": inst.health,
        "Attributes": inst.attributes,
    })
}

// ===== tag parsing + storage =====

/// Parse a `TagList` (`[{Key,Value}]`) into a key -> value map.
fn parse_tag_list(v: &Value) -> Result<BTreeMap<String, String>, AwsServiceError> {
    let arr = v
        .as_array()
        .ok_or_else(|| fault("InvalidInput", "Tags must be a list."))?;
    let mut out = BTreeMap::new();
    for t in arr {
        let key = req_str(t, "Key")?.to_string();
        let value = t.get("Value").and_then(Value::as_str).unwrap_or("");
        out.insert(key, value.to_string());
    }
    Ok(out)
}

/// Persist create-time `Tags` (present on Create*Namespace / CreateService)
/// under the resource ARN so `ListTagsForResource` reflects them.
fn store_create_tags(st: &mut crate::state::ServiceDiscoveryState, arn: &str, b: &Value) {
    let Some(tags) = b.get("Tags") else {
        return;
    };
    if let Ok(parsed) = parse_tag_list(tags) {
        if !parsed.is_empty() {
            st.tags.entry(arn.to_string()).or_default().extend(parsed);
        }
    }
}

/// Serialize a resource's stored tags as a `TagList` (`[{Key,Value}]`).
fn tag_list_json(tags: &crate::state::TagMap) -> Value {
    let list: Vec<Value> = tags
        .iter()
        .map(|(k, v)| json!({ "Key": k, "Value": v }))
        .collect();
    Value::Array(list)
}

/// Resolve a resource ARN (or bare id) to the canonical ARN of an existing
/// namespace or service, raising `ResourceNotFoundException` when unknown. Tags
/// are keyed by the canonical ARN so every taggable resource type is uniform.
fn resolve_taggable_arn(
    st: &crate::state::ServiceDiscoveryState,
    resource_arn: &str,
) -> Result<String, AwsServiceError> {
    let id = resource_id_from(resource_arn);
    if let Some(ns) = st.namespaces.get(id) {
        return Ok(ns.arn.clone());
    }
    if let Some(svc) = st.services.get(id) {
        return Ok(svc.arn.clone());
    }
    Err(fault(
        "ResourceNotFoundException",
        &format!("Resource {resource_arn} not found."),
    ))
}

// ===== namespace create (shared) =====

impl ServiceDiscoveryService {
    /// Shared body for the three Create*Namespace ops. `type_` is the
    /// `NamespaceType`; `dns` carries the synthesized DNS properties for
    /// public/private namespaces (`None` for HTTP).
    fn create_namespace(
        &self,
        req: &AwsRequest,
        type_: &str,
        dns: Option<DnsProps>,
        vpc: Option<String>,
        name_max: usize,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "Name")?.to_string();
        // HTTP names allow up to 1024 chars; DNS names up to 253.
        check_max_len(&name, name_max, "Name")?;
        if let Some(d) = b.get("Description").and_then(Value::as_str) {
            check_max_len(d, 1024, "Description")?;
        }
        // CreatorRequestId and Vpc are both `ResourceId` (max length 64) — AWS
        // rejects an over-long value with InvalidInput before doing any work.
        if let Some(crid) = b.get("CreatorRequestId").and_then(Value::as_str) {
            check_max_len(crid, 64, "CreatorRequestId")?;
        }
        if let Some(v) = &vpc {
            check_max_len(v, 64, "Vpc")?;
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // A private DNS namespace maps to a Route 53 private hosted zone
        // associated with the VPC. Creating a second one with the same name in
        // the same VPC conflicts: AWS still returns an OperationId, then the
        // operation settles to FAIL carrying Route 53's `ConflictingDomainExists`
        // (surfaced under the `CANNOT_CREATE_HOSTED_ZONE` operation error code).
        if type_ == "DNS_PRIVATE" {
            if let Some(v) = &vpc {
                let conflict = st.namespaces.values().any(|n| {
                    n.type_ == "DNS_PRIVATE"
                        && n.name == name
                        && n.vpc.as_deref() == Some(v.as_str())
                });
                if conflict {
                    let op_id = self.mint_conflicting_domain_operation(st, v, &name, &req.region);
                    return ok(json!({ "OperationId": op_id }));
                }
            }
        }
        // Namespace names are unique per account+type. AWS returns
        // NamespaceAlreadyExists synchronously for a duplicate HTTP/public-DNS
        // name. Private DNS namespaces are keyed by (name, VPC) and their
        // conflict surfaces asynchronously (a failed operation carrying the
        // Route 53 `ConflictingDomainExists`), so it's handled by the caller
        // before we get here.
        if type_ != "DNS_PRIVATE" && st.namespaces.values().any(|n| n.name == name) {
            return Err(fault(
                "NamespaceAlreadyExists",
                &format!("A namespace named {name} already exists."),
            ));
        }
        let id = new_namespace_id();
        let arn = namespace_arn(&req.region, &req.account_id, &id);
        let now = Utc::now();
        let ns = Namespace {
            id: id.clone(),
            arn: arn.clone(),
            name: name.clone(),
            type_: type_.to_string(),
            description: opt_str(&b, "Description"),
            service_count: 0,
            http_name: name,
            dns,
            vpc,
            creator_request_id: creator_request_id(&b),
            create_date: now,
        };
        st.namespaces.insert(id.clone(), ns);
        store_create_tags(st, &arn, &b);
        let op_id = self.mint_operation(st, "CREATE_NAMESPACE", "NAMESPACE", &id);
        ok(json!({ "OperationId": op_id }))
    }

    /// Insert a fresh `SUBMITTED` operation targeting `target_id` under the given
    /// `OperationTargetType` (`NAMESPACE`/`SERVICE`/`INSTANCE`) and return its id.
    /// Settles to `SUCCESS` on the first `GetOperation`.
    fn mint_operation(
        &self,
        st: &mut crate::state::ServiceDiscoveryState,
        type_: &str,
        target_type: &str,
        target_id: &str,
    ) -> String {
        let op_id = new_operation_id();
        let now = Utc::now();
        let mut targets = BTreeMap::new();
        targets.insert(target_type.to_string(), target_id.to_string());
        st.operations.insert(
            op_id.clone(),
            Operation {
                id: op_id.clone(),
                type_: type_.to_string(),
                status: "SUBMITTED".to_string(),
                error_message: None,
                error_code: None,
                create_date: now,
                update_date: now,
                targets,
            },
        );
        op_id
    }

    /// Mint a `CREATE_NAMESPACE` operation pre-settled to `FAIL` for a private
    /// DNS namespace whose (name, VPC) collides with an existing one. The failure
    /// mirrors Route 53's `ConflictingDomainExists`, surfaced under Cloud Map's
    /// `CANNOT_CREATE_HOSTED_ZONE` operation error code — the shape the Terraform
    /// provider parses out of a failed operation.
    fn mint_conflicting_domain_operation(
        &self,
        st: &mut crate::state::ServiceDiscoveryState,
        vpc: &str,
        name: &str,
        region: &str,
    ) -> String {
        let op_id = new_operation_id();
        let now = Utc::now();
        st.operations.insert(
            op_id.clone(),
            Operation {
                id: op_id.clone(),
                type_: "CREATE_NAMESPACE".to_string(),
                status: "FAIL".to_string(),
                error_code: Some("CANNOT_CREATE_HOSTED_ZONE".to_string()),
                error_message: Some(format!(
                    "The VPC that you chose, {vpc} in region {region}, is already \
                     associated with another private hosted zone that has an \
                     overlapping name space, {name}.. (Service: AmazonRoute53; \
                     Status Code: 400; Error Code: ConflictingDomainExists; \
                     Request ID: {op_id})"
                )),
                create_date: now,
                update_date: now,
                targets: BTreeMap::new(),
            },
        );
        op_id
    }
}

// ===== handlers =====

impl ServiceDiscoveryService {
    fn create_http_namespace(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.create_namespace(req, "HTTP", None, None, 1024)
    }

    fn create_public_dns_namespace(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let dns = DnsProps {
            hosted_zone_id: new_hosted_zone_id(),
            soa_ttl: soa_ttl_from_create(&b),
        };
        self.create_namespace(req, "DNS_PUBLIC", Some(dns), None, 253)
    }

    fn create_private_dns_namespace(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        // PrivateDns namespaces are VPC-scoped; Vpc is a required input.
        let vpc = req_str(&b, "Vpc")?.to_string();
        let dns = DnsProps {
            hosted_zone_id: new_hosted_zone_id(),
            soa_ttl: soa_ttl_from_create(&b),
        };
        self.create_namespace(req, "DNS_PRIVATE", Some(dns), Some(vpc), 253)
    }

    fn get_namespace(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = req_str(&b, "Id")?.to_string();
        let accounts = self.state.read();
        let Some(ns) = accounts
            .get(&req.account_id)
            .and_then(|s| s.namespaces.get(&id))
        else {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {id} not found."),
            ));
        };
        ok(json!({ "Namespace": namespace_json(ns, &req.account_id) }))
    }

    fn list_namespaces(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_pagination(&b)?;
        let accounts = self.state.read();
        let Some(st) = accounts.get(&req.account_id) else {
            return ok(json!({ "Namespaces": [] }));
        };
        // Present DNS namespaces (those carrying a hosted zone) ahead of
        // HTTP-only ones, breaking ties by id. AWS doesn't guarantee a
        // ListNamespaces order, so a stable deterministic ordering is faithful;
        // grouping the fuller DNS shape first also keeps the response's leading
        // element structurally representative of a DNS namespace.
        let mut namespaces: Vec<&Namespace> = st
            .namespaces
            .values()
            .filter(|ns| namespace_matches_filters(ns, &b, &req.account_id))
            .collect();
        namespaces.sort_by(|a, b| a.dns.is_none().cmp(&b.dns.is_none()).then(a.id.cmp(&b.id)));
        let items: Vec<Value> = namespaces
            .into_iter()
            .map(|ns| namespace_summary_json(ns, &req.account_id))
            .collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "Namespaces": page, "NextToken": next }))
    }

    fn delete_namespace(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = req_str(&b, "Id")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !st.namespaces.contains_key(&id) {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {id} not found."),
            ));
        }
        // A namespace that still owns services can't be deleted (ResourceInUse).
        // ServiceCount is 0 until the services batch lands, so this is a no-op
        // guard today but keeps the contract correct.
        if st.namespaces.get(&id).map(|n| n.service_count).unwrap_or(0) > 0 {
            return Err(fault(
                "ResourceInUse",
                &format!("Namespace {id} still contains services."),
            ));
        }
        st.namespaces.remove(&id);
        let op_id = self.mint_operation(st, "DELETE_NAMESPACE", "NAMESPACE", &id);
        ok(json!({ "OperationId": op_id }))
    }

    fn update_http_namespace(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = req_str(&b, "Id")?.to_string();
        let change = b
            .get("Namespace")
            .ok_or_else(|| fault("InvalidInput", "Namespace change block is required."))?;
        let description = opt_str(change, "Description");
        self.apply_namespace_update(req, &id, description, None)
    }

    fn update_public_dns_namespace(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = req_str(&b, "Id")?.to_string();
        let change = b
            .get("Namespace")
            .ok_or_else(|| fault("InvalidInput", "Namespace change block is required."))?;
        let description = opt_str(change, "Description");
        let soa_ttl = change
            .get("Properties")
            .and_then(|p| p.get("DnsProperties"))
            .and_then(|d| d.get("SOA"))
            .and_then(|s| s.get("TTL"))
            .and_then(Value::as_i64);
        self.apply_namespace_update(req, &id, description, soa_ttl)
    }

    fn update_private_dns_namespace(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Private and public DNS namespace updates carry the same change shape.
        self.update_public_dns_namespace(req)
    }

    /// Shared body for the three Update*Namespace ops: apply the mutable fields,
    /// then mint an `UPDATE_NAMESPACE` operation.
    fn apply_namespace_update(
        &self,
        req: &AwsRequest,
        id: &str,
        description: Option<String>,
        soa_ttl: Option<i64>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(ns) = st.namespaces.get_mut(id) else {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {id} not found."),
            ));
        };
        if let Some(d) = description {
            ns.description = Some(d);
        }
        if let (Some(ttl), Some(dns)) = (soa_ttl, ns.dns.as_mut()) {
            dns.soa_ttl = ttl;
        }
        let op_id = self.mint_operation(st, "UPDATE_NAMESPACE", "NAMESPACE", id);
        ok(json!({ "OperationId": op_id }))
    }

    fn get_operation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = req_str(&b, "OperationId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(op) = st.operations.get_mut(&id) else {
            return Err(fault(
                "OperationNotFound",
                &format!("Operation {id} not found."),
            ));
        };
        // Deterministic settle: a pending operation reports SUCCESS the moment
        // it's polled, mirroring Cloud Map's near-instant control-plane ops.
        if op.status == "SUBMITTED" || op.status == "PENDING" {
            op.status = "SUCCESS".to_string();
            op.update_date = Utc::now();
        }
        ok(json!({ "Operation": operation_json(op, &req.account_id) }))
    }

    fn list_operations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_pagination(&b)?;
        let accounts = self.state.read();
        let Some(st) = accounts.get(&req.account_id) else {
            return ok(json!({ "Operations": [] }));
        };
        let items: Vec<Value> = st
            .operations
            .values()
            .filter(|op| operation_matches_filters(op, &b))
            .map(|op| json!({ "Id": op.id, "Status": op.status }))
            .collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "Operations": page, "NextToken": next }))
    }
}

// ===== service handlers =====

impl ServiceDiscoveryService {
    /// CreateService is *synchronous* in Cloud Map: it returns the full
    /// `Service` shape immediately (no operation to poll), unlike the namespace
    /// create/update/delete calls.
    fn create_service(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let name = req_str(&b, "Name")?.to_string();
        check_max_len(&name, 127, "Name")?;
        // NamespaceId is semantically required (a service must live in a
        // namespace) even though the Smithy model leaves it optional. It may be
        // supplied as a bare id or a namespace ARN. DNS services carry it inside
        // `DnsConfig.NamespaceId` (the Terraform provider only sets the top-level
        // `NamespaceId` for HTTP services), so fall back to that block.
        let namespace_id = b
            .get("NamespaceId")
            .and_then(Value::as_str)
            .or_else(|| {
                b.get("DnsConfig")
                    .and_then(|d| d.get("NamespaceId"))
                    .and_then(Value::as_str)
            })
            .map(|s| resource_id_from(s).to_string())
            .ok_or_else(|| fault("InvalidInput", "NamespaceId is required."))?;
        if let Some(d) = b.get("Description").and_then(Value::as_str) {
            check_max_len(d, 1024, "Description")?;
        }
        let dns_config = match b.get("DnsConfig") {
            Some(v) => Some(parse_dns_config(v)?),
            None => None,
        };
        let health_check_config = match b.get("HealthCheckConfig") {
            Some(v) => Some(parse_health_check(v)?),
            None => None,
        };
        let health_check_custom_config = b
            .get("HealthCheckCustomConfig")
            .map(parse_health_check_custom);
        // An explicit `Type: HTTP` (ServiceTypeOption) forces an HTTP service.
        let explicit_http = b.get("Type").and_then(Value::as_str) == Some("HTTP");

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // The namespace must exist.
        let Some(ns) = st.namespaces.get(&namespace_id) else {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {namespace_id} not found."),
            ));
        };
        // Derive the service Type from the namespace kind and the presence of a
        // DnsConfig: HTTP namespaces (or an explicit HTTP option) yield `HTTP`;
        // a DNS namespace with DNS records yields `DNS_HTTP`.
        let type_ = if explicit_http || ns.type_ == "HTTP" {
            "HTTP".to_string()
        } else if dns_config.is_some() {
            "DNS_HTTP".to_string()
        } else {
            "HTTP".to_string()
        };
        // Service names are unique within a namespace.
        if st
            .services
            .values()
            .any(|s| s.namespace_id == namespace_id && s.name == name)
        {
            return Err(fault(
                "ServiceAlreadyExists",
                &format!("A service named {name} already exists."),
            ));
        }
        let id = new_service_id();
        let arn = service_arn(&req.region, &req.account_id, &id);
        let svc = Service {
            id: id.clone(),
            arn,
            name,
            namespace_id: namespace_id.clone(),
            type_,
            description: opt_str(&b, "Description"),
            instance_count: 0,
            dns_config,
            health_check_config,
            health_check_custom_config,
            attributes: BTreeMap::new(),
            creator_request_id: creator_request_id(&b),
            create_date: Utc::now(),
            instances: BTreeMap::new(),
            instances_revision: 0,
        };
        let arn = svc.arn.clone();
        st.services.insert(id.clone(), svc.clone());
        // Bump the parent namespace's ServiceCount.
        if let Some(ns) = st.namespaces.get_mut(&namespace_id) {
            ns.service_count += 1;
        }
        store_create_tags(st, &arn, &b);
        ok(json!({ "Service": service_json(&svc, &req.account_id) }))
    }

    fn get_service(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = resource_id_from(req_str(&b, "Id")?).to_string();
        let accounts = self.state.read();
        let Some(svc) = accounts
            .get(&req.account_id)
            .and_then(|s| s.services.get(&id))
        else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {id} not found."),
            ));
        };
        ok(json!({ "Service": service_json(svc, &req.account_id) }))
    }

    fn list_services(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_pagination(&b)?;
        let accounts = self.state.read();
        let Some(st) = accounts.get(&req.account_id) else {
            return ok(json!({ "Services": [] }));
        };
        let items: Vec<Value> = st
            .services
            .values()
            .filter(|svc| service_matches_filters(svc, &b, &req.account_id))
            .map(|svc| service_summary_json(svc, &req.account_id))
            .collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({ "Services": page, "NextToken": next }))
    }

    /// UpdateService is *asynchronous*: it returns an `OperationId` (unlike the
    /// synchronous create/delete on the same resource). The mutable fields
    /// (`Description`, `DnsConfig.DnsRecords`, `HealthCheckConfig`) are applied
    /// eagerly and an `UPDATE_SERVICE` operation is minted for the caller to poll.
    fn update_service(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = resource_id_from(req_str(&b, "Id")?).to_string();
        let change = b
            .get("Service")
            .ok_or_else(|| fault("InvalidInput", "Service change block is required."))?;
        let description = opt_str(change, "Description");
        let dns_records = match change.get("DnsConfig").and_then(|d| d.get("DnsRecords")) {
            Some(v) => Some(parse_dns_records(v)?),
            None => None,
        };
        let health_check = match change.get("HealthCheckConfig") {
            Some(v) => Some(parse_health_check(v)?),
            None => None,
        };
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(svc) = st.services.get_mut(&id) else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {id} not found."),
            ));
        };
        if let Some(d) = description {
            svc.description = Some(d);
        }
        if let Some(records) = dns_records {
            if let Some(dns) = svc.dns_config.as_mut() {
                dns.dns_records = records;
            }
        }
        if let Some(hc) = health_check {
            svc.health_check_config = Some(hc);
        }
        let op_id = self.mint_operation(st, "UPDATE_SERVICE", "SERVICE", &id);
        ok(json!({ "OperationId": op_id }))
    }

    /// DeleteService is *synchronous* (returns an empty body). A service that
    /// still owns registered instances can't be deleted (ResourceInUse); the
    /// instances batch has yet to land, so `instance_count` is 0 today, but the
    /// guard keeps the contract correct.
    fn delete_service(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = resource_id_from(req_str(&b, "Id")?).to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(svc) = st.services.get(&id) else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {id} not found."),
            ));
        };
        if svc.instance_count > 0 {
            return Err(fault(
                "ResourceInUse",
                &format!("Service {id} still contains registered instances."),
            ));
        }
        let namespace_id = svc.namespace_id.clone();
        st.services.remove(&id);
        // Decrement the parent namespace's ServiceCount.
        if let Some(ns) = st.namespaces.get_mut(&namespace_id) {
            ns.service_count = (ns.service_count - 1).max(0);
        }
        ok(json!({}))
    }

    fn get_service_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let accounts = self.state.read();
        let Some(svc) = accounts
            .get(&req.account_id)
            .and_then(|s| s.services.get(&id))
        else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {id} not found."),
            ));
        };
        ok(json!({ "ServiceAttributes": service_attributes_json(svc, &req.account_id) }))
    }

    fn update_service_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let attrs = b
            .get("Attributes")
            .and_then(Value::as_object)
            .ok_or_else(|| fault("InvalidInput", "Attributes is required."))?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(svc) = st.services.get_mut(&id) else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {id} not found."),
            ));
        };
        for (k, v) in attrs {
            if let Some(s) = v.as_str() {
                svc.attributes.insert(k.clone(), s.to_string());
            }
        }
        ok(json!({}))
    }

    fn delete_service_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let keys: Vec<String> = b
            .get("Attributes")
            .and_then(Value::as_array)
            .ok_or_else(|| fault("InvalidInput", "Attributes is required."))?
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(svc) = st.services.get_mut(&id) else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {id} not found."),
            ));
        };
        for k in keys {
            svc.attributes.remove(&k);
        }
        ok(json!({}))
    }
}

// ===== instance handlers =====

impl ServiceDiscoveryService {
    /// RegisterInstance is *asynchronous*: it stores the instance eagerly (so
    /// GetInstance sees it immediately) and returns a `REGISTER_INSTANCE`
    /// `OperationId` for the caller to poll.
    fn register_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let service_id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let instance_id = req_str(&b, "InstanceId")?.to_string();
        check_max_len(&instance_id, 64, "InstanceId")?;
        let attrs = parse_attributes(
            b.get("Attributes")
                .ok_or_else(|| fault("InvalidInput", "Attributes is required."))?,
        )?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(svc) = st.services.get_mut(&service_id) else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {service_id} not found."),
            ));
        };
        validate_instance_attributes(svc, &attrs)?;
        // Registering an id that already exists updates it in place (AWS treats a
        // re-register as an upsert); InstanceCount only grows for a new id.
        let is_new = !svc.instances.contains_key(&instance_id);
        svc.instances.insert(
            instance_id.clone(),
            Instance {
                id: instance_id.clone(),
                creator_request_id: creator_request_id(&b),
                attributes: attrs,
                health: "HEALTHY".to_string(),
            },
        );
        if is_new {
            svc.instance_count += 1;
        }
        svc.instances_revision += 1;
        let op_id =
            self.mint_instance_operation(st, "REGISTER_INSTANCE", &service_id, &instance_id);
        ok(json!({ "OperationId": op_id }))
    }

    /// DeregisterInstance is *asynchronous*: it removes the instance eagerly and
    /// returns a `DEREGISTER_INSTANCE` `OperationId`.
    fn deregister_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let service_id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let instance_id = req_str(&b, "InstanceId")?.to_string();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(svc) = st.services.get_mut(&service_id) else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {service_id} not found."),
            ));
        };
        if svc.instances.remove(&instance_id).is_none() {
            return Err(fault(
                "InstanceNotFound",
                &format!("Instance {instance_id} not found."),
            ));
        }
        svc.instance_count = (svc.instance_count - 1).max(0);
        svc.instances_revision += 1;
        let op_id =
            self.mint_instance_operation(st, "DEREGISTER_INSTANCE", &service_id, &instance_id);
        ok(json!({ "OperationId": op_id }))
    }

    /// Mint an instance-scoped operation whose `Targets` carry both the
    /// `INSTANCE` and owning `SERVICE` ids, matching the real API.
    fn mint_instance_operation(
        &self,
        st: &mut crate::state::ServiceDiscoveryState,
        type_: &str,
        service_id: &str,
        instance_id: &str,
    ) -> String {
        let op_id = new_operation_id();
        let now = Utc::now();
        let mut targets = BTreeMap::new();
        targets.insert("INSTANCE".to_string(), instance_id.to_string());
        targets.insert("SERVICE".to_string(), service_id.to_string());
        st.operations.insert(
            op_id.clone(),
            Operation {
                id: op_id.clone(),
                type_: type_.to_string(),
                status: "SUBMITTED".to_string(),
                error_message: None,
                error_code: None,
                create_date: now,
                update_date: now,
                targets,
            },
        );
        op_id
    }

    fn get_instance(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let service_id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let instance_id = req_str(&b, "InstanceId")?.to_string();
        let accounts = self.state.read();
        // An instance that lives in a service which no longer exists is itself
        // gone, so GetInstance reports `InstanceNotFound` (not `ServiceNotFound`)
        // when the parent service is absent. This matches how Cloud Map resolves
        // a deleted service on the instance read path and is what the Terraform
        // provider relies on to converge a destroy (it treats only
        // `InstanceNotFound` as "removed from state").
        let Some(inst) = accounts
            .get(&req.account_id)
            .and_then(|s| s.services.get(&service_id))
            .and_then(|svc| svc.instances.get(&instance_id))
        else {
            return Err(fault(
                "InstanceNotFound",
                &format!("Instance {instance_id} not found."),
            ));
        };
        ok(json!({
            "Instance": instance_json(inst, &req.account_id),
            "ResourceOwner": req.account_id,
        }))
    }

    fn list_instances(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_pagination(&b)?;
        let service_id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let accounts = self.state.read();
        let Some(svc) = accounts
            .get(&req.account_id)
            .and_then(|s| s.services.get(&service_id))
        else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {service_id} not found."),
            ));
        };
        let items: Vec<Value> = svc
            .instances
            .values()
            .map(|inst| instance_summary_json(inst, &req.account_id))
            .collect();
        let (page, next) = paginate(items, &b, 100);
        ok(json!({
            "Instances": page,
            "NextToken": next,
            "ResourceOwner": req.account_id,
        }))
    }

    fn get_instances_health_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        validate_pagination(&b)?;
        let service_id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        // An optional `Instances` list narrows the query to specific ids.
        let requested: Option<Vec<String>> =
            b.get("Instances").and_then(Value::as_array).map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            });
        let accounts = self.state.read();
        let Some(svc) = accounts
            .get(&req.account_id)
            .and_then(|s| s.services.get(&service_id))
        else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {service_id} not found."),
            ));
        };
        // When specific ids are named, every one must exist.
        if let Some(ids) = &requested {
            for id in ids {
                if !svc.instances.contains_key(id) {
                    return Err(fault(
                        "InstanceNotFound",
                        &format!("Instance {id} not found."),
                    ));
                }
            }
        }
        let mut items: Vec<Value> = svc
            .instances
            .values()
            .filter(|inst| {
                requested
                    .as_ref()
                    .is_none_or(|ids| ids.iter().any(|id| id == &inst.id))
            })
            .map(|inst| json!({ "Id": inst.id, "Status": inst.health }))
            .collect();
        // Paginate a flat list, then fold the page back into the status map.
        let (page, next) = paginate(std::mem::take(&mut items), &b, 100);
        let mut status = serde_json::Map::new();
        for entry in page {
            if let (Some(id), Some(st)) = (
                entry.get("Id").and_then(Value::as_str),
                entry.get("Status").and_then(Value::as_str),
            ) {
                status.insert(id.to_string(), json!(st));
            }
        }
        ok(json!({ "Status": Value::Object(status), "NextToken": next }))
    }

    fn update_instance_custom_health_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let service_id = resource_id_from(req_str(&b, "ServiceId")?).to_string();
        let instance_id = req_str(&b, "InstanceId")?.to_string();
        let status = req_str(&b, "Status")?.to_string();
        if status != "HEALTHY" && status != "UNHEALTHY" {
            return Err(fault(
                "InvalidInput",
                "Status must be HEALTHY or UNHEALTHY.",
            ));
        }
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let Some(svc) = st.services.get_mut(&service_id) else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {service_id} not found."),
            ));
        };
        // Custom health status only applies to a service configured with a
        // HealthCheckCustomConfig.
        if svc.health_check_custom_config.is_none() {
            return Err(fault(
                "CustomHealthNotFound",
                &format!("Service {service_id} has no custom health check configuration."),
            ));
        }
        let Some(inst) = svc.instances.get_mut(&instance_id) else {
            return Err(fault(
                "InstanceNotFound",
                &format!("Instance {instance_id} not found."),
            ));
        };
        inst.health = status;
        ok(json!({}))
    }

    /// DiscoverInstances is Cloud Map's *data-plane* lookup: resolve a namespace
    /// and service by name, then return the registered instances (filtered by
    /// `QueryParameters` attribute equality and the `HealthStatus` filter) as
    /// `HttpInstanceSummary` records, along with the service's revision counter.
    fn discover_instances(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let namespace_name = req_str(&b, "NamespaceName")?.to_string();
        let service_name = req_str(&b, "ServiceName")?.to_string();
        let query: BTreeMap<String, String> = match b.get("QueryParameters") {
            Some(v) => parse_attributes(v)?,
            None => BTreeMap::new(),
        };
        let health_filter =
            opt_str(&b, "HealthStatus").unwrap_or_else(|| "HEALTHY_OR_ELSE_ALL".to_string());
        let max = b
            .get("MaxResults")
            .and_then(Value::as_i64)
            .map(|n| n.clamp(1, 1000) as usize)
            .unwrap_or(100);
        let accounts = self.state.read();
        let Some(st) = accounts.get(&req.account_id) else {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {namespace_name} not found."),
            ));
        };
        let Some(ns) = st.namespaces.values().find(|n| n.name == namespace_name) else {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {namespace_name} not found."),
            ));
        };
        let Some(svc) = st
            .services
            .values()
            .find(|s| s.namespace_id == ns.id && s.name == service_name)
        else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {service_name} not found."),
            ));
        };
        // Attribute-equality filter: an instance matches only if it carries every
        // requested key with the requested value.
        let matching: Vec<&Instance> = svc
            .instances
            .values()
            .filter(|inst| {
                query
                    .iter()
                    .all(|(k, v)| inst.attributes.get(k).map(|a| a == v).unwrap_or(false))
            })
            .collect();
        // HealthStatus filter. HEALTHY_OR_ELSE_ALL returns healthy instances, or
        // all of them when none are healthy (AWS fail-open semantics).
        let selected: Vec<&Instance> = match health_filter.as_str() {
            "HEALTHY" => matching
                .iter()
                .copied()
                .filter(|i| i.health == "HEALTHY")
                .collect(),
            "UNHEALTHY" => matching
                .iter()
                .copied()
                .filter(|i| i.health == "UNHEALTHY")
                .collect(),
            "HEALTHY_OR_ELSE_ALL" => {
                let healthy: Vec<&Instance> = matching
                    .iter()
                    .copied()
                    .filter(|i| i.health == "HEALTHY")
                    .collect();
                if healthy.is_empty() {
                    matching.clone()
                } else {
                    healthy
                }
            }
            // "ALL" (and any unrecognized value) returns every matching instance.
            _ => matching.clone(),
        };
        let instances: Vec<Value> = selected
            .into_iter()
            .take(max)
            .map(|inst| http_instance_summary_json(inst, &ns.name, &svc.name))
            .collect();
        ok(json!({
            "Instances": instances,
            "InstancesRevision": svc.instances_revision,
        }))
    }

    fn discover_instances_revision(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let namespace_name = req_str(&b, "NamespaceName")?.to_string();
        let service_name = req_str(&b, "ServiceName")?.to_string();
        let accounts = self.state.read();
        let Some(st) = accounts.get(&req.account_id) else {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {namespace_name} not found."),
            ));
        };
        let Some(ns) = st.namespaces.values().find(|n| n.name == namespace_name) else {
            return Err(fault(
                "NamespaceNotFound",
                &format!("Namespace {namespace_name} not found."),
            ));
        };
        let Some(svc) = st
            .services
            .values()
            .find(|s| s.namespace_id == ns.id && s.name == service_name)
        else {
            return Err(fault(
                "ServiceNotFound",
                &format!("Service {service_name} not found."),
            ));
        };
        ok(json!({ "InstancesRevision": svc.instances_revision }))
    }
}

// ===== tag handlers =====

impl ServiceDiscoveryService {
    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let resource_arn = req_str(&b, "ResourceARN")?.to_string();
        let tags = parse_tag_list(
            b.get("Tags")
                .ok_or_else(|| fault("InvalidInput", "Tags is required."))?,
        )?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let arn = resolve_taggable_arn(st, &resource_arn)?;
        st.tags.entry(arn).or_default().extend(tags);
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let resource_arn = req_str(&b, "ResourceARN")?.to_string();
        let keys: Vec<String> = b
            .get("TagKeys")
            .and_then(Value::as_array)
            .ok_or_else(|| fault("InvalidInput", "TagKeys is required."))?
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let arn = resolve_taggable_arn(st, &resource_arn)?;
        if let Some(map) = st.tags.get_mut(&arn) {
            for k in &keys {
                map.remove(k);
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let resource_arn = req_str(&b, "ResourceARN")?.to_string();
        let accounts = self.state.read();
        let Some(st) = accounts.get(&req.account_id) else {
            return Err(fault(
                "ResourceNotFoundException",
                &format!("Resource {resource_arn} not found."),
            ));
        };
        let arn = resolve_taggable_arn(st, &resource_arn)?;
        let empty = crate::state::TagMap::new();
        let tags = st.tags.get(&arn).unwrap_or(&empty);
        ok(json!({ "Tags": tag_list_json(tags) }))
    }
}

// ===== filters =====

/// Apply a single filter `Values`/`Condition` against a candidate string.
fn filter_hit(values: &[String], condition: &str, candidate: &str) -> bool {
    match condition {
        "BEGINS_WITH" => values.iter().any(|v| candidate.starts_with(v.as_str())),
        // EQ / IN both reduce to membership; a lone value makes EQ, a set makes
        // IN. BETWEEN is only meaningful for the UPDATE_DATE numeric filter,
        // handled separately by the caller.
        _ => values.iter().any(|v| v == candidate),
    }
}

fn filter_values(f: &Value) -> Vec<String> {
    f.get("Values")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn namespace_matches_filters(ns: &Namespace, b: &Value, owner: &str) -> bool {
    let Some(filters) = b.get("Filters").and_then(Value::as_array) else {
        return true;
    };
    for f in filters {
        let name = f.get("Name").and_then(Value::as_str).unwrap_or("");
        let condition = f.get("Condition").and_then(Value::as_str).unwrap_or("EQ");
        let values = filter_values(f);
        let candidate = match name {
            "TYPE" => ns.type_.as_str(),
            "NAME" => ns.name.as_str(),
            "HTTP_NAME" => ns.http_name.as_str(),
            // RESOURCE_OWNER filters by owning account. Every namespace here is
            // owned by the calling account, so a filter naming a different
            // account excludes everything.
            "RESOURCE_OWNER" => owner,
            _ => continue,
        };
        if !filter_hit(&values, condition, candidate) {
            return false;
        }
    }
    true
}

fn operation_matches_filters(op: &Operation, b: &Value) -> bool {
    let Some(filters) = b.get("Filters").and_then(Value::as_array) else {
        return true;
    };
    for f in filters {
        let name = f.get("Name").and_then(Value::as_str).unwrap_or("");
        let condition = f.get("Condition").and_then(Value::as_str).unwrap_or("EQ");
        let values = filter_values(f);
        let candidate = match name {
            "STATUS" => op.status.clone(),
            "TYPE" => op.type_.clone(),
            "NAMESPACE_ID" => op.targets.get("NAMESPACE").cloned().unwrap_or_default(),
            "SERVICE_ID" => op.targets.get("SERVICE").cloned().unwrap_or_default(),
            // UPDATE_DATE is a BETWEEN filter over epoch millis; not exercised
            // by the namespace batch, so it's accepted without narrowing.
            "UPDATE_DATE" => continue,
            _ => continue,
        };
        if !filter_hit(&values, condition, &candidate) {
            return false;
        }
    }
    true
}

/// ListServices supports the `NAMESPACE_ID` and `RESOURCE_OWNER` filters.
fn service_matches_filters(svc: &Service, b: &Value, owner: &str) -> bool {
    let Some(filters) = b.get("Filters").and_then(Value::as_array) else {
        return true;
    };
    for f in filters {
        let name = f.get("Name").and_then(Value::as_str).unwrap_or("");
        let condition = f.get("Condition").and_then(Value::as_str).unwrap_or("EQ");
        let values = filter_values(f);
        let candidate = match name {
            "NAMESPACE_ID" => svc.namespace_id.as_str(),
            // Every service here is owned by the calling account, so a filter
            // naming a different account excludes everything.
            "RESOURCE_OWNER" => owner,
            _ => continue,
        };
        if !filter_hit(&values, condition, candidate) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> ServiceDiscoveryService {
        ServiceDiscoveryService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "servicediscovery".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    /// `Result::unwrap_err` requires `AwsResponse: Debug` (it isn't), so pull the
    /// error out by hand.
    fn expect_err(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error response"),
            Err(e) => e,
        }
    }

    #[test]
    fn create_get_list_update_delete_lifecycle() {
        let s = svc();
        // Create HTTP namespace -> get an OperationId back.
        let resp = s
            .create_http_namespace(&req(
                "CreateHttpNamespace",
                json!({ "Name": "myns", "Description": "d" }),
            ))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        assert!(!op_id.is_empty());

        // GetOperation settles SUBMITTED -> SUCCESS and targets the namespace.
        let resp = s
            .get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
            .unwrap();
        let op = &body_of(&resp)["Operation"];
        assert_eq!(op["Status"], "SUCCESS");
        assert_eq!(op["Type"], "CREATE_NAMESPACE");
        let ns_id = op["Targets"]["NAMESPACE"].as_str().unwrap().to_string();
        assert!(ns_id.starts_with("ns-"));

        // Namespace is visible via GetNamespace.
        let resp = s
            .get_namespace(&req("GetNamespace", json!({ "Id": ns_id })))
            .unwrap();
        let ns = &body_of(&resp)["Namespace"];
        assert_eq!(ns["Name"], "myns");
        assert_eq!(ns["Type"], "HTTP");
        assert_eq!(ns["Properties"]["HttpProperties"]["HttpName"], "myns");
        assert!(ns["Properties"].get("DnsProperties").is_none());

        // ListNamespaces sees it.
        let resp = s
            .list_namespaces(&req("ListNamespaces", json!({})))
            .unwrap();
        assert_eq!(body_of(&resp)["Namespaces"].as_array().unwrap().len(), 1);

        // Update description.
        s.update_http_namespace(&req(
            "UpdateHttpNamespace",
            json!({ "Id": ns_id, "Namespace": { "Description": "new" } }),
        ))
        .unwrap();
        let resp = s
            .get_namespace(&req("GetNamespace", json!({ "Id": ns_id })))
            .unwrap();
        assert_eq!(body_of(&resp)["Namespace"]["Description"], "new");

        // Delete -> namespace gone.
        s.delete_namespace(&req("DeleteNamespace", json!({ "Id": ns_id })))
            .unwrap();
        let err = expect_err(s.get_namespace(&req("GetNamespace", json!({ "Id": ns_id }))));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn public_dns_namespace_has_dns_properties() {
        let s = svc();
        let resp = s
            .create_public_dns_namespace(&req(
                "CreatePublicDnsNamespace",
                json!({ "Name": "pub.example.com" }),
            ))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        let resp = s
            .get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
            .unwrap();
        let ns_id = body_of(&resp)["Operation"]["Targets"]["NAMESPACE"]
            .as_str()
            .unwrap()
            .to_string();
        let resp = s
            .get_namespace(&req("GetNamespace", json!({ "Id": ns_id })))
            .unwrap();
        let dns = &body_of(&resp)["Namespace"]["Properties"]["DnsProperties"];
        assert!(dns["HostedZoneId"].as_str().unwrap().starts_with('Z'));
        assert_eq!(dns["SOA"]["TTL"], DEFAULT_SOA_TTL);
    }

    #[test]
    fn private_dns_requires_vpc() {
        let s = svc();
        let err = expect_err(s.create_private_dns_namespace(&req(
            "CreatePrivateDnsNamespace",
            json!({ "Name": "priv.internal" }),
        )));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn duplicate_name_rejected() {
        let s = svc();
        s.create_public_dns_namespace(&req(
            "CreatePublicDnsNamespace",
            json!({ "Name": "dup.example.com" }),
        ))
        .unwrap();
        let err = expect_err(s.create_public_dns_namespace(&req(
            "CreatePublicDnsNamespace",
            json!({ "Name": "dup.example.com" }),
        )));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    /// A private DNS namespace with the same name in the *same* VPC conflicts:
    /// the create still returns an OperationId, but the operation settles to FAIL
    /// carrying Route 53's `ConflictingDomainExists` (under `CANNOT_CREATE_HOSTED_ZONE`).
    #[test]
    fn private_dns_conflicting_domain_fails_operation() {
        let s = svc();
        let first = s
            .create_private_dns_namespace(&req(
                "CreatePrivateDnsNamespace",
                json!({ "Name": "priv.internal", "Vpc": "vpc-abc" }),
            ))
            .unwrap();
        // Settle the first create (SUCCESS).
        let op1 = body_of(&first)["OperationId"].as_str().unwrap().to_string();
        s.get_operation(&req("GetOperation", json!({ "OperationId": op1 })))
            .unwrap();

        // Same name + same VPC -> conflict op.
        let resp = s
            .create_private_dns_namespace(&req(
                "CreatePrivateDnsNamespace",
                json!({ "Name": "priv.internal", "Vpc": "vpc-abc" }),
            ))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        let op = body_of(
            &s.get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
                .unwrap(),
        );
        let op = &op["Operation"];
        assert_eq!(op["Status"], "FAIL");
        assert_eq!(op["ErrorCode"], "CANNOT_CREATE_HOSTED_ZONE");
        assert!(op["ErrorMessage"]
            .as_str()
            .unwrap()
            .contains("ConflictingDomainExists"));

        // Same name in a *different* VPC is allowed (own hosted zone).
        s.create_private_dns_namespace(&req(
            "CreatePrivateDnsNamespace",
            json!({ "Name": "priv.internal", "Vpc": "vpc-xyz" }),
        ))
        .unwrap();
    }

    /// An over-long CreatorRequestId / Vpc (both `ResourceId`, max 64) is rejected
    /// with InvalidInput before any hosted-zone work.
    #[test]
    fn private_dns_rejects_overlong_resource_ids() {
        let s = svc();
        let long = "x".repeat(65);
        let err = expect_err(s.create_private_dns_namespace(&req(
            "CreatePrivateDnsNamespace",
            json!({ "Name": "priv.internal", "Vpc": long }),
        )));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        let long = "x".repeat(65);
        let err = expect_err(s.create_http_namespace(&req(
            "CreateHttpNamespace",
            json!({ "Name": "n", "CreatorRequestId": long }),
        )));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    /// The Terraform provider puts the namespace id inside `DnsConfig.NamespaceId`
    /// for DNS services (not the top-level `NamespaceId`); CreateService must
    /// accept it there and echo it back both top-level and in DnsConfig.
    #[test]
    fn create_service_namespace_id_from_dns_config() {
        let s = svc();
        let ns_id = make_namespace(&s, "svc.ns");
        let resp = s
            .create_service(&req(
                "CreateService",
                json!({
                    "Name": "web",
                    "DnsConfig": {
                        "NamespaceId": ns_id,
                        "RoutingPolicy": "MULTIVALUE",
                        "DnsRecords": [ { "Type": "A", "TTL": 60 } ],
                    },
                }),
            ))
            .unwrap();
        let svc_json = &body_of(&resp)["Service"];
        assert_eq!(svc_json["NamespaceId"], ns_id);
        assert_eq!(svc_json["DnsConfig"]["NamespaceId"], ns_id);
    }

    #[test]
    fn unknown_operation_and_namespace_404() {
        let s = svc();
        let err = expect_err(
            s.get_operation(&req("GetOperation", json!({ "OperationId": "nope-nope" }))),
        );
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        let err = expect_err(s.get_namespace(&req("GetNamespace", json!({ "Id": "ns-missing" }))));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    /// Create an HTTP namespace and return its id (settling the create op).
    fn make_namespace(s: &ServiceDiscoveryService, name: &str) -> String {
        let resp = s
            .create_http_namespace(&req("CreateHttpNamespace", json!({ "Name": name })))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        let resp = s
            .get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
            .unwrap();
        body_of(&resp)["Operation"]["Targets"]["NAMESPACE"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Create a DNS namespace and return its id.
    fn make_dns_namespace(s: &ServiceDiscoveryService, name: &str) -> String {
        let resp = s
            .create_public_dns_namespace(&req("CreatePublicDnsNamespace", json!({ "Name": name })))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        let resp = s
            .get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
            .unwrap();
        body_of(&resp)["Operation"]["Targets"]["NAMESPACE"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn ns_service_count(s: &ServiceDiscoveryService, ns_id: &str) -> i64 {
        let resp = s
            .get_namespace(&req("GetNamespace", json!({ "Id": ns_id })))
            .unwrap();
        body_of(&resp)["Namespace"]["ServiceCount"]
            .as_i64()
            .unwrap()
    }

    #[test]
    fn service_full_lifecycle() {
        let s = svc();
        let ns_id = make_dns_namespace(&s, "svc.example.com");

        // CreateService is synchronous — returns the Service directly.
        let resp = s
            .create_service(&req(
                "CreateService",
                json!({
                    "Name": "myservice",
                    "NamespaceId": ns_id,
                    "Description": "d",
                    "DnsConfig": {
                        "RoutingPolicy": "MULTIVALUE",
                        "DnsRecords": [{ "Type": "A", "TTL": 60 }],
                    },
                    "HealthCheckConfig": { "Type": "HTTP", "ResourcePath": "/", "FailureThreshold": 2 },
                }),
            ))
            .unwrap();
        let created = &body_of(&resp)["Service"];
        let srv_id = created["Id"].as_str().unwrap().to_string();
        assert!(srv_id.starts_with("srv-"));
        assert_eq!(created["Name"], "myservice");
        assert_eq!(created["NamespaceId"], ns_id);
        assert_eq!(created["Type"], "DNS_HTTP");
        assert_eq!(created["InstanceCount"], 0);
        assert_eq!(created["DnsConfig"]["DnsRecords"][0]["TTL"], 60);
        assert_eq!(created["HealthCheckConfig"]["FailureThreshold"], 2);
        assert!(created["Arn"].as_str().unwrap().contains(":service/srv-"));

        // Namespace ServiceCount incremented.
        assert_eq!(ns_service_count(&s, &ns_id), 1);

        // GetService round-trips.
        let resp = s
            .get_service(&req("GetService", json!({ "Id": srv_id })))
            .unwrap();
        assert_eq!(body_of(&resp)["Service"]["Name"], "myservice");

        // ListServices with NAMESPACE_ID filter.
        let resp = s
            .list_services(&req(
                "ListServices",
                json!({ "Filters": [{ "Name": "NAMESPACE_ID", "Condition": "EQ", "Values": [ns_id] }] }),
            ))
            .unwrap();
        let list = body_of(&resp);
        assert_eq!(list["Services"].as_array().unwrap().len(), 1);
        assert_eq!(list["Services"][0]["Id"], srv_id);
        // A different namespace id filters everything out.
        let resp = s
            .list_services(&req(
                "ListServices",
                json!({ "Filters": [{ "Name": "NAMESPACE_ID", "Values": ["ns-other"] }] }),
            ))
            .unwrap();
        assert_eq!(body_of(&resp)["Services"].as_array().unwrap().len(), 0);

        // UpdateService is asynchronous — returns an OperationId that settles.
        let resp = s
            .update_service(&req(
                "UpdateService",
                json!({
                    "Id": srv_id,
                    "Service": {
                        "Description": "updated",
                        "DnsConfig": { "DnsRecords": [{ "Type": "A", "TTL": 120 }] },
                    },
                }),
            ))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        let resp = s
            .get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
            .unwrap();
        let op = &body_of(&resp)["Operation"];
        assert_eq!(op["Status"], "SUCCESS");
        assert_eq!(op["Type"], "UPDATE_SERVICE");
        assert_eq!(op["Targets"]["SERVICE"], srv_id);
        let resp = s
            .get_service(&req("GetService", json!({ "Id": srv_id })))
            .unwrap();
        let updated = body_of(&resp);
        assert_eq!(updated["Service"]["Description"], "updated");
        assert_eq!(updated["Service"]["DnsConfig"]["DnsRecords"][0]["TTL"], 120);

        // Attributes: set -> get -> delete.
        s.update_service_attributes(&req(
            "UpdateServiceAttributes",
            json!({ "ServiceId": srv_id, "Attributes": { "port": "80", "protocol": "HTTP" } }),
        ))
        .unwrap();
        let resp = s
            .get_service_attributes(&req("GetServiceAttributes", json!({ "ServiceId": srv_id })))
            .unwrap();
        let attrs = body_of(&resp);
        assert_eq!(attrs["ServiceAttributes"]["ServiceArn"], created["Arn"]);
        assert_eq!(attrs["ServiceAttributes"]["Attributes"]["port"], "80");
        s.delete_service_attributes(&req(
            "DeleteServiceAttributes",
            json!({ "ServiceId": srv_id, "Attributes": ["port"] }),
        ))
        .unwrap();
        let resp = s
            .get_service_attributes(&req("GetServiceAttributes", json!({ "ServiceId": srv_id })))
            .unwrap();
        let attrs = body_of(&resp);
        assert!(attrs["ServiceAttributes"]["Attributes"]
            .get("port")
            .is_none());
        assert_eq!(attrs["ServiceAttributes"]["Attributes"]["protocol"], "HTTP");

        // DeleteService is synchronous (empty body) and decrements ServiceCount.
        s.delete_service(&req("DeleteService", json!({ "Id": srv_id })))
            .unwrap();
        assert_eq!(ns_service_count(&s, &ns_id), 0);
        let err = expect_err(s.get_service(&req("GetService", json!({ "Id": srv_id }))));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn create_service_by_namespace_arn() {
        let s = svc();
        let ns_id = make_namespace(&s, "httpns");
        let ns_arn = format!("arn:aws:servicediscovery:us-east-1:000000000000:namespace/{ns_id}");
        // HTTP namespace yields an HTTP-typed service.
        let resp = s
            .create_service(&req(
                "CreateService",
                json!({ "Name": "s1", "NamespaceId": ns_arn }),
            ))
            .unwrap();
        assert_eq!(body_of(&resp)["Service"]["Type"], "HTTP");
        assert_eq!(body_of(&resp)["Service"]["NamespaceId"], ns_id);
    }

    #[test]
    fn create_service_missing_namespace_404() {
        let s = svc();
        let err = expect_err(s.create_service(&req(
            "CreateService",
            json!({ "Name": "orphan", "NamespaceId": "ns-doesnotexist" }),
        )));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn duplicate_service_name_rejected() {
        let s = svc();
        let ns_id = make_namespace(&s, "dupns");
        s.create_service(&req(
            "CreateService",
            json!({ "Name": "dup", "NamespaceId": ns_id }),
        ))
        .unwrap();
        let err = expect_err(s.create_service(&req(
            "CreateService",
            json!({ "Name": "dup", "NamespaceId": ns_id }),
        )));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn service_ops_404_on_missing() {
        let s = svc();
        for r in [
            s.get_service(&req("GetService", json!({ "Id": "srv-missing" }))),
            s.update_service(&req(
                "UpdateService",
                json!({ "Id": "srv-missing", "Service": { "Description": "x" } }),
            )),
            s.delete_service(&req("DeleteService", json!({ "Id": "srv-missing" }))),
            s.get_service_attributes(&req(
                "GetServiceAttributes",
                json!({ "ServiceId": "srv-missing" }),
            )),
            s.update_service_attributes(&req(
                "UpdateServiceAttributes",
                json!({ "ServiceId": "srv-missing", "Attributes": { "a": "b" } }),
            )),
            s.delete_service_attributes(&req(
                "DeleteServiceAttributes",
                json!({ "ServiceId": "srv-missing", "Attributes": ["a"] }),
            )),
        ] {
            assert_eq!(expect_err(r).status(), StatusCode::NOT_FOUND);
        }
    }

    /// Create a DNS namespace + a DNS_HTTP service (A record) and return
    /// `(namespace_name, namespace_id, service_id)`.
    fn make_dns_service(
        s: &ServiceDiscoveryService,
        ns_name: &str,
        svc_name: &str,
    ) -> (String, String) {
        let ns_id = make_dns_namespace(s, ns_name);
        let resp = s
            .create_service(&req(
                "CreateService",
                json!({
                    "Name": svc_name,
                    "NamespaceId": ns_id,
                    "DnsConfig": {
                        "RoutingPolicy": "MULTIVALUE",
                        "DnsRecords": [{ "Type": "A", "TTL": 60 }],
                    },
                }),
            ))
            .unwrap();
        let srv_id = body_of(&resp)["Service"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        (ns_id, srv_id)
    }

    /// RegisterInstance -> settle op -> GetInstance visible, InstanceCount++.
    fn register(s: &ServiceDiscoveryService, srv_id: &str, inst_id: &str, attrs: Value) -> String {
        let resp = s
            .register_instance(&req(
                "RegisterInstance",
                json!({ "ServiceId": srv_id, "InstanceId": inst_id, "Attributes": attrs }),
            ))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        let resp = s
            .get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
            .unwrap();
        let op = &body_of(&resp)["Operation"];
        assert_eq!(op["Status"], "SUCCESS");
        assert_eq!(op["Type"], "REGISTER_INSTANCE");
        assert_eq!(op["Targets"]["INSTANCE"], inst_id);
        assert_eq!(op["Targets"]["SERVICE"], srv_id);
        op_id
    }

    fn svc_instance_count(s: &ServiceDiscoveryService, srv_id: &str) -> i64 {
        let resp = s
            .get_service(&req("GetService", json!({ "Id": srv_id })))
            .unwrap();
        body_of(&resp)["Service"]["InstanceCount"].as_i64().unwrap()
    }

    #[test]
    fn instance_full_lifecycle() {
        let s = svc();
        let (_ns_id, srv_id) = make_dns_service(&s, "inst.example.com", "web");

        // Register -> InstanceCount goes to 1, GetInstance sees it.
        register(
            &s,
            &srv_id,
            "i-1",
            json!({ "AWS_INSTANCE_IPV4": "10.0.0.1" }),
        );
        assert_eq!(svc_instance_count(&s, &srv_id), 1);
        let resp = s
            .get_instance(&req(
                "GetInstance",
                json!({ "ServiceId": srv_id, "InstanceId": "i-1" }),
            ))
            .unwrap();
        let inst = &body_of(&resp)["Instance"];
        assert_eq!(inst["Id"], "i-1");
        assert_eq!(inst["Attributes"]["AWS_INSTANCE_IPV4"], "10.0.0.1");

        // A second instance -> ListInstances returns both.
        register(
            &s,
            &srv_id,
            "i-2",
            json!({ "AWS_INSTANCE_IPV4": "10.0.0.2" }),
        );
        assert_eq!(svc_instance_count(&s, &srv_id), 2);
        let resp = s
            .list_instances(&req("ListInstances", json!({ "ServiceId": srv_id })))
            .unwrap();
        assert_eq!(body_of(&resp)["Instances"].as_array().unwrap().len(), 2);

        // Health status defaults to HEALTHY.
        let resp = s
            .get_instances_health_status(&req(
                "GetInstancesHealthStatus",
                json!({ "ServiceId": srv_id }),
            ))
            .unwrap();
        assert_eq!(body_of(&resp)["Status"]["i-1"], "HEALTHY");
        assert_eq!(body_of(&resp)["Status"]["i-2"], "HEALTHY");

        // Deregister i-2 -> InstanceCount back to 1.
        let resp = s
            .deregister_instance(&req(
                "DeregisterInstance",
                json!({ "ServiceId": srv_id, "InstanceId": "i-2" }),
            ))
            .unwrap();
        let op_id = body_of(&resp)["OperationId"].as_str().unwrap().to_string();
        let resp = s
            .get_operation(&req("GetOperation", json!({ "OperationId": op_id })))
            .unwrap();
        assert_eq!(body_of(&resp)["Operation"]["Type"], "DEREGISTER_INSTANCE");
        assert_eq!(svc_instance_count(&s, &srv_id), 1);
        let err = expect_err(s.get_instance(&req(
            "GetInstance",
            json!({ "ServiceId": srv_id, "InstanceId": "i-2" }),
        )));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn register_instance_requires_record_attribute() {
        let s = svc();
        let (_ns_id, srv_id) = make_dns_service(&s, "req.example.com", "api");
        // Missing AWS_INSTANCE_IPV4 for the A record -> InvalidInput.
        let err = expect_err(s.register_instance(&req(
            "RegisterInstance",
            json!({ "ServiceId": srv_id, "InstanceId": "bad", "Attributes": { "custom": "x" } }),
        )));
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn register_instance_missing_service_404() {
        let s = svc();
        let err = expect_err(s.register_instance(&req(
            "RegisterInstance",
            json!({ "ServiceId": "srv-missing", "InstanceId": "i", "Attributes": { "AWS_INSTANCE_IPV4": "1.2.3.4" } }),
        )));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn custom_health_status_lifecycle() {
        let s = svc();
        let ns_id = make_namespace(&s, "customhealth");
        // HTTP service with a custom health check config.
        let resp = s
            .create_service(&req(
                "CreateService",
                json!({
                    "Name": "chk",
                    "NamespaceId": ns_id,
                    "HealthCheckCustomConfig": { "FailureThreshold": 1 },
                }),
            ))
            .unwrap();
        let srv_id = body_of(&resp)["Service"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        register(
            &s,
            &srv_id,
            "ci-1",
            json!({ "AWS_INSTANCE_IPV4": "10.1.1.1" }),
        );

        // Update custom health to UNHEALTHY -> reflected in GetInstancesHealthStatus.
        s.update_instance_custom_health_status(&req(
            "UpdateInstanceCustomHealthStatus",
            json!({ "ServiceId": srv_id, "InstanceId": "ci-1", "Status": "UNHEALTHY" }),
        ))
        .unwrap();
        let resp = s
            .get_instances_health_status(&req(
                "GetInstancesHealthStatus",
                json!({ "ServiceId": srv_id, "Instances": ["ci-1"] }),
            ))
            .unwrap();
        assert_eq!(body_of(&resp)["Status"]["ci-1"], "UNHEALTHY");
    }

    #[test]
    fn custom_health_on_non_custom_service_404() {
        let s = svc();
        let (_ns_id, srv_id) = make_dns_service(&s, "nch.example.com", "svc");
        register(
            &s,
            &srv_id,
            "i-1",
            json!({ "AWS_INSTANCE_IPV4": "10.0.0.1" }),
        );
        let err = expect_err(s.update_instance_custom_health_status(&req(
            "UpdateInstanceCustomHealthStatus",
            json!({ "ServiceId": srv_id, "InstanceId": "i-1", "Status": "HEALTHY" }),
        )));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn discover_instances_by_name_filter_and_revision() {
        let s = svc();
        let (_ns_id, srv_id) = make_dns_service(&s, "disc.example.com", "web");
        register(
            &s,
            &srv_id,
            "i-1",
            json!({ "AWS_INSTANCE_IPV4": "10.0.0.1", "stage": "prod" }),
        );
        register(
            &s,
            &srv_id,
            "i-2",
            json!({ "AWS_INSTANCE_IPV4": "10.0.0.2", "stage": "dev" }),
        );

        // Discover all -> both, revision == 2 (two registers).
        let resp = s
            .discover_instances(&req(
                "DiscoverInstances",
                json!({ "NamespaceName": "disc.example.com", "ServiceName": "web" }),
            ))
            .unwrap();
        let out = body_of(&resp);
        assert_eq!(out["Instances"].as_array().unwrap().len(), 2);
        assert_eq!(out["InstancesRevision"], 2);
        let first = &out["Instances"][0];
        assert_eq!(first["NamespaceName"], "disc.example.com");
        assert_eq!(first["ServiceName"], "web");
        assert_eq!(first["HealthStatus"], "HEALTHY");

        // QueryParameters attribute-equality filter narrows to i-2.
        let resp = s
            .discover_instances(&req(
                "DiscoverInstances",
                json!({
                    "NamespaceName": "disc.example.com",
                    "ServiceName": "web",
                    "QueryParameters": { "stage": "dev" },
                }),
            ))
            .unwrap();
        let out = body_of(&resp);
        assert_eq!(out["Instances"].as_array().unwrap().len(), 1);
        assert_eq!(out["Instances"][0]["InstanceId"], "i-2");

        // DiscoverInstancesRevision returns the same counter.
        let resp = s
            .discover_instances_revision(&req(
                "DiscoverInstancesRevision",
                json!({ "NamespaceName": "disc.example.com", "ServiceName": "web" }),
            ))
            .unwrap();
        assert_eq!(body_of(&resp)["InstancesRevision"], 2);

        // Deregister bumps the revision.
        s.deregister_instance(&req(
            "DeregisterInstance",
            json!({ "ServiceId": srv_id, "InstanceId": "i-1" }),
        ))
        .unwrap();
        let resp = s
            .discover_instances_revision(&req(
                "DiscoverInstancesRevision",
                json!({ "NamespaceName": "disc.example.com", "ServiceName": "web" }),
            ))
            .unwrap();
        assert_eq!(body_of(&resp)["InstancesRevision"], 3);
    }

    #[test]
    fn discover_instances_missing_namespace_404() {
        let s = svc();
        let err = expect_err(s.discover_instances(&req(
            "DiscoverInstances",
            json!({ "NamespaceName": "nope", "ServiceName": "web" }),
        )));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn tag_round_trip_on_namespace_and_service() {
        let s = svc();
        let ns_id = make_namespace(&s, "tagns");
        let ns_arn = format!("arn:aws:servicediscovery:us-east-1:000000000000:namespace/{ns_id}");
        let resp = s
            .create_service(&req(
                "CreateService",
                json!({ "Name": "tagsvc", "NamespaceId": ns_id, "Tags": [{ "Key": "team", "Value": "core" }] }),
            ))
            .unwrap();
        let svc_arn = body_of(&resp)["Service"]["Arn"]
            .as_str()
            .unwrap()
            .to_string();

        // Create-time service tag is reflected in ListTagsForResource.
        let resp = s
            .list_tags_for_resource(&req(
                "ListTagsForResource",
                json!({ "ResourceARN": svc_arn }),
            ))
            .unwrap();
        let tags = body_of(&resp)["Tags"].as_array().unwrap().clone();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "team");

        // TagResource on the namespace ARN.
        s.tag_resource(&req(
            "TagResource",
            json!({ "ResourceARN": ns_arn, "Tags": [{ "Key": "env", "Value": "prod" }, { "Key": "owner", "Value": "a" }] }),
        ))
        .unwrap();
        let resp = s
            .list_tags_for_resource(&req(
                "ListTagsForResource",
                json!({ "ResourceARN": ns_arn }),
            ))
            .unwrap();
        assert_eq!(body_of(&resp)["Tags"].as_array().unwrap().len(), 2);

        // UntagResource removes one key.
        s.untag_resource(&req(
            "UntagResource",
            json!({ "ResourceARN": ns_arn, "TagKeys": ["owner"] }),
        ))
        .unwrap();
        let resp = s
            .list_tags_for_resource(&req(
                "ListTagsForResource",
                json!({ "ResourceARN": ns_arn }),
            ))
            .unwrap();
        let tags = body_of(&resp)["Tags"].as_array().unwrap().clone();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "env");
    }

    #[test]
    fn tag_unknown_resource_404() {
        let s = svc();
        for r in [
            s.tag_resource(&req(
                "TagResource",
                json!({ "ResourceARN": "arn:aws:servicediscovery:us-east-1:000000000000:namespace/ns-missing", "Tags": [{ "Key": "a", "Value": "b" }] }),
            )),
            s.untag_resource(&req(
                "UntagResource",
                json!({ "ResourceARN": "srv-missing", "TagKeys": ["a"] }),
            )),
            s.list_tags_for_resource(&req(
                "ListTagsForResource",
                json!({ "ResourceARN": "ns-missing" }),
            )),
        ] {
            assert_eq!(expect_err(r).status(), StatusCode::NOT_FOUND);
        }
    }
}
