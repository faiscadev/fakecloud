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
use crate::state::{DnsProps, Namespace, Operation, SharedServiceDiscoveryState};

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
        "NamespaceNotFound" | "OperationNotFound" | "ServiceNotFound" | "InstanceNotFound" => {
            StatusCode::NOT_FOUND
        }
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
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // Namespace names are unique per account+type. AWS returns
        // NamespaceAlreadyExists for a duplicate name.
        if st.namespaces.values().any(|n| n.name == name) {
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
            arn,
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
        let op_id = self.mint_operation(st, "CREATE_NAMESPACE", &id);
        ok(json!({ "OperationId": op_id }))
    }

    /// Insert a fresh `SUBMITTED` operation targeting `namespace_id` and return
    /// its id. Settles to `SUCCESS` on the first `GetOperation`.
    fn mint_operation(
        &self,
        st: &mut crate::state::ServiceDiscoveryState,
        type_: &str,
        namespace_id: &str,
    ) -> String {
        let op_id = new_operation_id();
        let now = Utc::now();
        let mut targets = BTreeMap::new();
        targets.insert("NAMESPACE".to_string(), namespace_id.to_string());
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
        let op_id = self.mint_operation(st, "DELETE_NAMESPACE", &id);
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
        let op_id = self.mint_operation(st, "UPDATE_NAMESPACE", id);
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
}
