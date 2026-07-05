//! Amazon MQ (`mq`) restJson1 dispatch + operation handlers.
//!
//! The full 25-operation Amazon MQ control plane. Requests are routed to an
//! operation by HTTP method + `@http` URI path; path labels are captured
//! positionally and query parameters are read from the raw query string so
//! repeated multi-value keys survive. State is account-partitioned and
//! persisted; each broker and configuration is stored as its already-output-
//! valid wire object so `Describe` echoes exactly what `Create`/`Update`
//! persisted, while a broker's `brokerInstances` (per-engine endpoints) and
//! `users` summary are derived at describe time from live state.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::runtime::{BrokerUser, MqEngine, MqRuntime};
use crate::shared;
use crate::state::{BrokerDataPlane, MqData, SharedMqState};

/// Every operation name in the Amazon MQ Smithy model (25 operations).
pub const MQ_ACTIONS: &[&str] = &[
    "CreateBroker",
    "CreateConfiguration",
    "CreateTags",
    "CreateUser",
    "DeleteBroker",
    "DeleteConfiguration",
    "DeleteTags",
    "DeleteUser",
    "DescribeBroker",
    "DescribeBrokerEngineTypes",
    "DescribeBrokerInstanceOptions",
    "DescribeConfiguration",
    "DescribeConfigurationRevision",
    "DescribeSharedResources",
    "DescribeUser",
    "ListBrokers",
    "ListConfigurationRevisions",
    "ListConfigurations",
    "ListTags",
    "ListUsers",
    "Promote",
    "RebootBroker",
    "UpdateBroker",
    "UpdateConfiguration",
    "UpdateUser",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "CreateBroker",
    "CreateConfiguration",
    "CreateTags",
    "CreateUser",
    "DeleteBroker",
    "DeleteConfiguration",
    "DeleteTags",
    "DeleteUser",
    "Promote",
    "RebootBroker",
    "UpdateBroker",
    "UpdateConfiguration",
    "UpdateUser",
    // NB: DescribeBroker is intentionally NOT here. It can settle an in-flight
    // lifecycle transition, but a pure read that settles nothing must not write
    // a snapshot (avoiding per-read disk amplification). When a describe DOES
    // fire a transition, `dispatch` reports it via the `settled` flag and the
    // handler persists then.
];

pub struct MqService {
    state: SharedMqState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    /// The backing-container runtime. `None` in the degraded / control-plane-
    /// only mode (no Docker/Podman): brokers still reach a state via the
    /// in-memory lifecycle but no real engine container is spawned. When
    /// present, the runtime owns the CREATION/REBOOT/DELETION transitions and
    /// a broker is only `RUNNING` once its container accepts connections.
    runtime: Option<Arc<MqRuntime>>,
}

impl MqService {
    pub fn new(state: SharedMqState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            runtime: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Attach the backing-container runtime that spawns real ActiveMQ/RabbitMQ
    /// brokers. Without it, MQ runs control-plane-only.
    pub fn with_runtime(mut self, runtime: Arc<MqRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Whether the in-memory lifecycle should auto-settle broker transitions.
    /// `true` only when no container runtime is attached; otherwise the
    /// runtime's background tasks own every transition.
    fn auto_settle(&self) -> bool {
        self.runtime.is_none()
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
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let put = m == Method::PUT;
        let del = m == Method::DELETE;
        let l = |v: &str| vec![v.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["v1", "brokers"] if post => ("CreateBroker", vec![]),
            ["v1", "brokers"] if get => ("ListBrokers", vec![]),
            ["v1", "broker-engine-types"] if get => ("DescribeBrokerEngineTypes", vec![]),
            ["v1", "broker-instance-options"] if get => ("DescribeBrokerInstanceOptions", vec![]),
            ["v1", "brokers", id] if get => ("DescribeBroker", l(id)),
            ["v1", "brokers", id] if put => ("UpdateBroker", l(id)),
            ["v1", "brokers", id] if del => ("DeleteBroker", l(id)),
            ["v1", "brokers", id, "reboot"] if post => ("RebootBroker", l(id)),
            ["v1", "brokers", id, "promote"] if post => ("Promote", l(id)),
            ["v1", "brokers", id, "shared-resources"] if get => ("DescribeSharedResources", l(id)),
            ["v1", "brokers", id, "users"] if get => ("ListUsers", l(id)),
            ["v1", "brokers", id, "users", user] if post => {
                ("CreateUser", vec![id.to_string(), user.to_string()])
            }
            ["v1", "brokers", id, "users", user] if get => {
                ("DescribeUser", vec![id.to_string(), user.to_string()])
            }
            ["v1", "brokers", id, "users", user] if put => {
                ("UpdateUser", vec![id.to_string(), user.to_string()])
            }
            ["v1", "brokers", id, "users", user] if del => {
                ("DeleteUser", vec![id.to_string(), user.to_string()])
            }
            ["v1", "configurations"] if post => ("CreateConfiguration", vec![]),
            ["v1", "configurations"] if get => ("ListConfigurations", vec![]),
            ["v1", "configurations", id] if get => ("DescribeConfiguration", l(id)),
            ["v1", "configurations", id] if put => ("UpdateConfiguration", l(id)),
            ["v1", "configurations", id] if del => ("DeleteConfiguration", l(id)),
            ["v1", "configurations", id, "revisions"] if get => {
                ("ListConfigurationRevisions", l(id))
            }
            ["v1", "configurations", id, "revisions", rev] if get => (
                "DescribeConfigurationRevision",
                vec![id.to_string(), rev.to_string()],
            ),
            ["v1", "tags", arn] if post => ("CreateTags", l(arn)),
            ["v1", "tags", arn] if get => ("ListTags", l(arn)),
            ["v1", "tags", arn] if del => ("DeleteTags", l(arn)),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for MqService {
    fn service_name(&self) -> &str {
        "mq"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let (result, settled) = self.dispatch(action, &labels, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        // Persist when a mutating op succeeded, or when a read settled a real
        // lifecycle transition (a describe that changed nothing does not write).
        if settled || (MUTATING.contains(&action) && success) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        MQ_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl MqService {
    /// Returns the operation result plus a `settled` flag: `true` when a read
    /// (`DescribeBroker`) fired a real broker lifecycle transition and the
    /// change must be persisted.
    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> (Result<AwsResponse, AwsServiceError>, bool) {
        let body = match parse_body(req) {
            Ok(b) => b,
            Err(e) => return (Err(e), false),
        };
        if let Err(e) = crate::validate::validate_input(action, &body) {
            return (Err(e), false);
        }
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        if let Err(e) = crate::validate::validate_query(&q) {
            return (Err(e), false);
        }
        let a0 = labels.first().map(String::as_str).unwrap_or_default();
        let a1 = labels.get(1).map(String::as_str).unwrap_or_default();
        let mut settled = false;
        let result = match action {
            "CreateBroker" => self.create_broker(&ctx, &body),
            "DescribeBroker" => self.describe_broker(&ctx, a0, &mut settled),
            "UpdateBroker" => self.update_broker(&ctx, a0, &body),
            "DeleteBroker" => self.delete_broker(&ctx, a0),
            "RebootBroker" => self.reboot_broker(&ctx, a0),
            "Promote" => self.promote(&ctx, a0, &body),
            "ListBrokers" => self.list_brokers(&ctx, &q),
            "DescribeSharedResources" => self.describe_shared_resources(&ctx, a0),
            "CreateConfiguration" => self.create_configuration(&ctx, &body),
            "DescribeConfiguration" => self.describe_configuration(&ctx, a0),
            "UpdateConfiguration" => self.update_configuration(&ctx, a0, &body),
            "DeleteConfiguration" => self.delete_configuration(&ctx, a0),
            "ListConfigurations" => self.list_configurations(&ctx, &q),
            "ListConfigurationRevisions" => self.list_configuration_revisions(&ctx, a0, &q),
            "DescribeConfigurationRevision" => self.describe_configuration_revision(&ctx, a0, a1),
            "CreateUser" => self.create_user(&ctx, a0, a1, &body),
            "DescribeUser" => self.describe_user(&ctx, a0, a1),
            "UpdateUser" => self.update_user(&ctx, a0, a1, &body),
            "DeleteUser" => self.delete_user(&ctx, a0, a1),
            "ListUsers" => self.list_users(&ctx, a0, &q),
            "CreateTags" => self.create_tags(&ctx, a0, &body),
            "DeleteTags" => self.delete_tags(&ctx, a0, &q),
            "ListTags" => self.list_tags(&ctx, a0),
            "DescribeBrokerEngineTypes" => self.describe_broker_engine_types(&q),
            "DescribeBrokerInstanceOptions" => self.describe_broker_instance_options(&q),
            _ => Err(AwsServiceError::action_not_implemented("mq", action)),
        };
        (result, settled)
    }
}

// ===================== helpers =====================

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

/// A 204 No Content response (the `CreateTags` / `DeleteTags` HTTP code).
fn no_content() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::NO_CONTENT, Vec::new()))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| bad_request(&format!("Request body is malformed: {e}")))
}

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg)
}

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg)
}

fn now_iso() -> String {
    shared::now_iso()
}

fn config_arn(ctx: &Ctx, id: &str) -> String {
    shared::config_arn(&ctx.region, &ctx.account, id)
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

fn query_all(q: &[(String, String)], key: &str) -> Vec<String> {
    q.iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .collect()
}

/// The effective page size for a list operation: the caller's `maxResults`
/// (validated to 1..=100 upstream) or AWS's default of 100. Echoed verbatim in
/// the responses whose wire shape carries a `maxResults` member.
fn page_size(q: &[(String, String)]) -> usize {
    query_one(q, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(100)
}

/// Paginate a slice by `maxResults` (default 100) + numeric `nextToken` offset.
/// Returns the page plus the next token (an offset), if more remain.
fn paginate(items: &[Value], q: &[(String, String)]) -> (Vec<Value>, Option<String>) {
    let max = page_size(q);
    let start = query_one(q, "nextToken")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + max).min(items.len());
    let page = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

/// Derive a broker's `brokerInstances` (per-engine wire endpoints, console URL,
/// and IP) from live state. Only present once the broker is `RUNNING`.
/// Projected from the single `shared::broker_endpoints` source so the describe
/// view and the CFN `Fn::GetAtt` list attributes stay in lock-step.
fn broker_instances(
    id: &str,
    engine: &str,
    region: &str,
    deployment_mode: &str,
    data_plane: Option<&BrokerDataPlane>,
) -> Value {
    let e = shared::broker_endpoints(id, engine, region, deployment_mode, data_plane);
    let mut out = Vec::new();
    for i in 0..e.ips.len() {
        let mut endpoints = Vec::new();
        if let Some(v) = e.open_wire.get(i) {
            endpoints.push(json!(v));
        }
        if let Some(v) = e.amqp.get(i) {
            endpoints.push(json!(v));
        }
        if let Some(v) = e.stomp.get(i) {
            endpoints.push(json!(v));
        }
        if let Some(v) = e.mqtt.get(i) {
            endpoints.push(json!(v));
        }
        if let Some(v) = e.wss.get(i) {
            endpoints.push(json!(v));
        }
        out.push(json!({
            "consoleURL": e.console_urls.get(i).cloned().unwrap_or_default(),
            "endpoints": endpoints,
            "ipAddress": e.ips.get(i).cloned().unwrap_or_default(),
        }));
    }
    Value::Array(out)
}

/// The engine, users, and configuration needed to bring a broker's backing
/// container up. Derived from live state so the create, reboot, and restart-
/// recovery paths all provision identically.
struct BrokerSpec {
    engine: MqEngine,
    users: Vec<BrokerUser>,
    /// Decoded configuration data (`activemq.xml` / `rabbitmq.conf`), if the
    /// broker has one associated.
    user_config: Option<String>,
}

/// Whether a broker runs the RabbitMQ engine (user changes apply immediately)
/// vs ActiveMQ (staged for reboot).
fn broker_is_rabbit(data: &MqData, id: &str) -> bool {
    data.brokers
        .get(id)
        .and_then(|b| b.get("engineType"))
        .and_then(Value::as_str)
        .map(shared::is_rabbit)
        .unwrap_or(false)
}

/// Project a `RunningBroker` into the persisted, describe-facing data-plane
/// binding.
fn running_to_dp(r: &crate::runtime::RunningBroker) -> BrokerDataPlane {
    BrokerDataPlane {
        container_id: r.container_id.clone(),
        host: r.host.clone(),
        ports: r.ports.clone(),
    }
}

/// Gather the container-provisioning spec for a broker from live state,
/// reflecting any staged pending changes: users staged for deletion are
/// excluded, and the pending configuration (if any) wins over the current one.
/// This makes the create path (no pending changes), the reboot path (apply
/// pending), and the restart-recovery path provision from one source.
fn gather_spec(data: &MqData, id: &str) -> Option<BrokerSpec> {
    let broker = data.brokers.get(id)?;
    let engine = MqEngine::from_wire(
        broker
            .get("engineType")
            .and_then(Value::as_str)
            .unwrap_or("ACTIVEMQ"),
    );
    let mut users = Vec::new();
    if let Some(map) = data.users.get(id) {
        for (name, u) in map {
            if u.get("pendingChange").and_then(Value::as_str) == Some("DELETE") {
                continue;
            }
            users.push(BrokerUser {
                username: name.clone(),
                password: u
                    .get("password")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                groups: u
                    .get("groups")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|g| g.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default(),
                console_access: u
                    .get("consoleAccess")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }
    }
    let cfg_ref = broker
        .get("configurations")
        .and_then(|c| c.get("pending").or_else(|| c.get("current")));
    let user_config = cfg_ref.and_then(|c| {
        let cid = c.get("id").and_then(Value::as_str)?;
        let rev = c.get("revision").and_then(Value::as_i64)?;
        let revs = data.configuration_revisions.get(cid)?;
        let entry = revs
            .iter()
            .find(|r| r.get("revision").and_then(Value::as_i64) == Some(rev))?;
        let b64 = entry.get("data").and_then(Value::as_str)?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
        String::from_utf8(bytes).ok()
    });
    Some(BrokerSpec {
        engine,
        users,
        user_config,
    })
}

// ===================== brokers =====================

impl MqService {
    fn create_broker(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = b
            .get("brokerName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let creator = b
            .get("creatorRequestId")
            .and_then(Value::as_str)
            .map(str::to_string);

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);

        // creatorRequestId is MQ's idempotency token: a repeat returns the same broker.
        if let Some(cr) = &creator {
            if let Some(existing) = data.broker_creator_ids.get(cr).cloned() {
                let arn = data
                    .brokers
                    .get(&existing)
                    .and_then(|x| x.get("brokerArn"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                return ok(json!({ "brokerId": existing, "brokerArn": arn }));
            }
        }
        // Broker names are unique within an account+REGION (a same-named broker
        // in a different region is fine, and does not conflict here).
        if data.brokers.values().any(|x| {
            x.get("brokerName").and_then(Value::as_str) == Some(name.as_str())
                && shared::broker_region(x) == Some(ctx.region.as_str())
        }) {
            return Err(conflict(&format!(
                "Broker name {name} already exists. Please choose a different name."
            )));
        }

        // Single shared create path (same one the CFN provisioner reuses).
        let (id, arn) = shared::create_broker_record(data, &ctx.account, &ctx.region, b);
        if let Some(cr) = creator {
            data.broker_creator_ids.insert(cr, id.clone());
        }

        // Background-spawn the real backing engine container (never in the
        // request handler -- a synchronous pull/start would time the client
        // out, #1539/#1730). The broker stays CREATION_IN_PROGRESS and only
        // settles to RUNNING once the container accepts connections. With no
        // runtime attached, the in-memory lifecycle settles it instead.
        if let Some(spec) = gather_spec(data, &id) {
            drop(guard);
            self.spawn_create(ctx.account.clone(), id.clone(), spec);
        }

        ok(json!({ "brokerId": id, "brokerArn": arn }))
    }

    /// Spawn the backing container for a freshly-created broker and settle it
    /// to `RUNNING` (recording the real host/port map) once reachable, or to
    /// `CREATION_FAILED` if the container can't come up. Reaps the container if
    /// the broker was deleted mid-create.
    fn spawn_create(&self, account: String, id: String, spec: BrokerSpec) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            let result = runtime
                .ensure_broker(&id, spec.engine, &spec.users, spec.user_config.as_deref())
                .await;
            match result {
                Ok(running) => {
                    let present = {
                        let mut guard = state.write();
                        let data = guard.get_or_create(&account);
                        if let Some(obj) = data.brokers.get_mut(&id).and_then(Value::as_object_mut)
                        {
                            obj.insert("brokerState".into(), json!("RUNNING"));
                            data.data_plane.insert(id.clone(), running_to_dp(&running));
                            true
                        } else {
                            false
                        }
                    };
                    if !present {
                        // Deleted mid-create: don't leak the container.
                        runtime.stop_broker(&id).await;
                    }
                }
                Err(error) => {
                    tracing::error!(%error, broker_id = %id, "MQ broker container failed to start");
                    let mut guard = state.write();
                    let data = guard.get_or_create(&account);
                    if let Some(obj) = data.brokers.get_mut(&id).and_then(Value::as_object_mut) {
                        obj.insert("brokerState".into(), json!("CREATION_FAILED"));
                    }
                }
            }
            save_snapshot(&state, store, &lock).await;
        });
    }

    fn describe_broker(
        &self,
        ctx: &Ctx,
        id: &str,
        settled: &mut bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // Settle any in-flight lifecycle transition; report whether one fired so
        // the handler persists only when the read actually changed state.
        *settled = data.reconcile_brokers(self.auto_settle());
        let mut broker = data
            .brokers
            .get(id)
            .cloned()
            .ok_or_else(|| not_found(&format!("Can't find requested broker [{id}].")))?;
        let obj = broker.as_object_mut().expect("broker is an object");
        let engine = obj
            .get("engineType")
            .and_then(Value::as_str)
            .unwrap_or("ACTIVEMQ")
            .to_string();
        let deployment = obj
            .get("deploymentMode")
            .and_then(Value::as_str)
            .unwrap_or("SINGLE_INSTANCE")
            .to_string();
        let state = obj
            .get("brokerState")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        // brokerInstances are served once the broker is running, projecting the
        // REAL host + mapped ports from the live data-plane binding when a
        // backing container is up (falling back to the cosmetic synthesis in
        // the control-plane-only path -- same shape either way).
        let data_plane = data.data_plane.get(id).cloned();
        if state == "RUNNING" {
            obj.insert(
                "brokerInstances".into(),
                broker_instances(id, &engine, &ctx.region, &deployment, data_plane.as_ref()),
            );
        }
        // users summary is derived from live per-broker user state.
        obj.insert("users".into(), users_summary(data, id));
        obj.insert("actionsRequired".into(), json!([]));
        // Tags are rendered from the single ARN-keyed tag map at read time, so
        // CreateTags/DeleteTags made after create are always reflected here
        // (no frozen create-time copy that drifts).
        let arn = obj
            .get("brokerArn")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if let Some(t) = data.tags.get(&arn).filter(|m| !m.is_empty()) {
            obj.insert("tags".into(), render_tags(t));
        }
        ok(Value::Object(obj.clone()))
    }

    fn update_broker(
        &self,
        ctx: &Ctx,
        id: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers(self.auto_settle());
        let broker = data
            .brokers
            .get_mut(id)
            .ok_or_else(|| not_found(&format!("Can't find requested broker [{id}].")))?;
        let obj = broker.as_object_mut().expect("broker is an object");
        // Stage the requested changes as pending; a reboot applies them.
        if let Some(v) = b.get("engineVersion") {
            obj.insert("pendingEngineVersion".into(), v.clone());
        }
        if let Some(v) = b.get("hostInstanceType") {
            obj.insert("pendingHostInstanceType".into(), v.clone());
        }
        if let Some(v) = b.get("authenticationStrategy") {
            obj.insert("pendingAuthenticationStrategy".into(), v.clone());
        }
        if let Some(v) = b.get("securityGroups") {
            obj.insert("pendingSecurityGroups".into(), v.clone());
        }
        if let Some(v) = b.get("autoMinorVersionUpgrade") {
            obj.insert("autoMinorVersionUpgrade".into(), v.clone());
        }
        if let Some(v) = b.get("maintenanceWindowStartTime") {
            obj.insert("maintenanceWindowStartTime".into(), v.clone());
        }
        if let Some(v) = b.get("dataReplicationMode") {
            obj.insert("dataReplicationMode".into(), v.clone());
        }
        if let Some(logs) = b.get("logs") {
            let entry = obj.entry("logs").or_insert_with(|| json!({}));
            if let Some(le) = entry.as_object_mut() {
                let pending = json!({
                    "audit": logs.get("audit").cloned().unwrap_or(json!(false)),
                    "general": logs.get("general").cloned().unwrap_or(json!(false)),
                });
                le.insert("pending".into(), pending);
            }
        }
        if let Some(cfg) = b.get("configuration") {
            let configs = obj
                .entry("configurations")
                .or_insert_with(|| json!({ "history": [] }));
            if let Some(ce) = configs.as_object_mut() {
                ce.insert("pending".into(), cfg.clone());
            }
        }
        // The response echoes the (now-effective-or-pending) broker attributes.
        let resp = json!({
            "brokerId": id,
            "authenticationStrategy": obj.get("authenticationStrategy").cloned().unwrap_or(json!("SIMPLE")),
            "autoMinorVersionUpgrade": obj.get("autoMinorVersionUpgrade").cloned().unwrap_or(json!(false)),
            "configuration": b.get("configuration").cloned().unwrap_or(json!(null)),
            "engineVersion": b.get("engineVersion").cloned().or_else(|| obj.get("engineVersion").cloned()).unwrap_or(json!(null)),
            "hostInstanceType": b.get("hostInstanceType").cloned().or_else(|| obj.get("hostInstanceType").cloned()).unwrap_or(json!(null)),
            "logs": b.get("logs").cloned().unwrap_or(json!(null)),
            "maintenanceWindowStartTime": obj.get("maintenanceWindowStartTime").cloned().unwrap_or(json!(null)),
            "securityGroups": b.get("securityGroups").cloned().or_else(|| obj.get("securityGroups").cloned()).unwrap_or(json!([])),
            "dataReplicationMode": obj.get("dataReplicationMode").cloned().unwrap_or(json!("NONE")),
        });
        ok(resp)
    }

    fn delete_broker(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers(self.auto_settle());
        let broker = data
            .brokers
            .get_mut(id)
            .ok_or_else(|| not_found(&format!("Can't find requested broker [{id}].")))?;
        broker
            .as_object_mut()
            .expect("broker is an object")
            .insert("brokerState".into(), json!("DELETION_IN_PROGRESS"));
        // With a runtime, stop + remove the real container and free state in a
        // background task; without one, the in-memory reconcile removes it on
        // the next describe.
        if self.runtime.is_some() {
            drop(guard);
            self.spawn_delete(ctx.account.clone(), id.to_string());
        }
        ok(json!({ "brokerId": id }))
    }

    /// Tear down a broker's backing container and free its state.
    fn spawn_delete(&self, account: String, id: String) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            runtime.stop_broker(&id).await;
            {
                let mut guard = state.write();
                let data = guard.get_or_create(&account);
                data.brokers.remove(&id);
                data.users.remove(&id);
                data.data_plane.remove(&id);
                data.broker_creator_ids.retain(|_, v| v != &id);
            }
            save_snapshot(&state, store, &lock).await;
        });
    }

    fn reboot_broker(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers(self.auto_settle());
        let broker = data
            .brokers
            .get_mut(id)
            .ok_or_else(|| not_found(&format!("Can't find requested broker [{id}].")))?;
        broker
            .as_object_mut()
            .expect("broker is an object")
            .insert("brokerState".into(), json!("REBOOT_IN_PROGRESS"));
        // With a runtime, recreate the container applying the staged pending
        // users/configuration (matching AWS applying changes on reboot), then
        // settle back to RUNNING once reachable. Without one, the in-memory
        // reconcile applies pending + settles on the next describe.
        if self.runtime.is_some() {
            // Compute the post-reboot spec (pending changes applied) WITHOUT
            // mutating state yet, so the broker stays REBOOT_IN_PROGRESS until
            // the fresh container is ready.
            if let Some(spec) = gather_spec(data, id) {
                drop(guard);
                self.spawn_reboot(ctx.account.clone(), id.to_string(), spec);
            }
        }
        ok(json!({}))
    }

    /// Recreate a broker's backing container with its post-reboot users/config,
    /// then apply the staged pending changes in state and settle to `RUNNING`.
    fn spawn_reboot(&self, account: String, id: String, spec: BrokerSpec) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            match runtime
                .reboot_broker(&id, spec.engine, &spec.users, spec.user_config.as_deref())
                .await
            {
                Ok(running) => {
                    let mut guard = state.write();
                    let data = guard.get_or_create(&account);
                    if data.brokers.contains_key(&id) {
                        // apply_pending drains pending users/config into the
                        // live fields and flips REBOOT_IN_PROGRESS -> RUNNING.
                        data.apply_pending(&id);
                        data.data_plane.insert(id.clone(), running_to_dp(&running));
                    }
                }
                Err(error) => {
                    tracing::error!(%error, broker_id = %id, "MQ broker reboot failed");
                    let mut guard = state.write();
                    let data = guard.get_or_create(&account);
                    if let Some(obj) = data.brokers.get_mut(&id).and_then(Value::as_object_mut) {
                        obj.insert("brokerState".into(), json!("RUNNING"));
                    }
                }
            }
            save_snapshot(&state, store, &lock).await;
        });
    }

    /// Recreate the backing containers for persisted brokers after a restart.
    /// Same bug class as RDS #1338 / ElastiCache: without this, a broker reads
    /// back `RUNNING` while its container is gone, so the endpoint is dead.
    /// Fire-and-forget: one task per broker, so a slow broker bring-up doesn't
    /// block startup.
    pub async fn recover_persisted_containers(&self) {
        let Some(_runtime) = self.runtime.clone() else {
            return;
        };
        struct Pending {
            account: String,
            id: String,
            spec: BrokerSpec,
        }
        let pending: Vec<Pending> = {
            let mut guard = self.state.write();
            let mut out = Vec::new();
            for (account_id, data) in guard.iter_mut() {
                let account = account_id.to_string();
                let ids: Vec<String> = data.brokers.keys().cloned().collect();
                for id in ids {
                    let st = data
                        .brokers
                        .get(&id)
                        .and_then(|b| b.get("brokerState"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    // Re-drive anything that should be backed by a live
                    // container (RUNNING, or a transition interrupted by the
                    // restart). Deletions are dropped; failed creates stay
                    // failed.
                    let recoverable = matches!(
                        st.as_str(),
                        "RUNNING" | "CREATION_IN_PROGRESS" | "REBOOT_IN_PROGRESS"
                    );
                    if !recoverable {
                        if st == "DELETION_IN_PROGRESS" {
                            data.brokers.remove(&id);
                            data.users.remove(&id);
                            data.data_plane.remove(&id);
                        }
                        continue;
                    }
                    // Mark CREATION_IN_PROGRESS so a describe during recovery
                    // doesn't advertise a dead endpoint; the container isn't up.
                    data.data_plane.remove(&id);
                    if let Some(obj) = data.brokers.get_mut(&id).and_then(Value::as_object_mut) {
                        obj.insert("brokerState".into(), json!("CREATION_IN_PROGRESS"));
                    }
                    if let Some(spec) = gather_spec(data, &id) {
                        out.push(Pending {
                            account: account.clone(),
                            id,
                            spec,
                        });
                    }
                }
            }
            out
        };
        if pending.is_empty() {
            return;
        }
        tracing::info!(
            count = pending.len(),
            "recovering backing containers for persisted mq brokers",
        );
        for p in pending {
            self.spawn_create(p.account, p.id, p.spec);
        }
    }

    /// Stop every backing broker container (graceful shutdown / reset).
    pub async fn stop_all_containers(&self) {
        if let Some(rt) = &self.runtime {
            rt.stop_all().await;
        }
    }

    fn promote(&self, ctx: &Ctx, id: &str, _b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers(self.auto_settle());
        if !data.brokers.contains_key(id) {
            return Err(not_found(&format!("Can't find requested broker [{id}].")));
        }
        ok(json!({ "brokerId": id }))
    }

    fn list_brokers(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers(self.auto_settle());
        // ListBrokers is scoped to the request's region (a broker created in
        // another region must not leak into this region's listing).
        let summaries: Vec<Value> = data
            .brokers
            .values()
            .filter(|b| shared::broker_region(b) == Some(ctx.region.as_str()))
            .map(|b| {
                json!({
                    "brokerArn": b.get("brokerArn").cloned().unwrap_or(json!("")),
                    "brokerId": b.get("brokerId").cloned().unwrap_or(json!("")),
                    "brokerName": b.get("brokerName").cloned().unwrap_or(json!("")),
                    "brokerState": b.get("brokerState").cloned().unwrap_or(json!("")),
                    "created": b.get("created").cloned().unwrap_or(json!("")),
                    "deploymentMode": b.get("deploymentMode").cloned().unwrap_or(json!("SINGLE_INSTANCE")),
                    "engineType": b.get("engineType").cloned().unwrap_or(json!("ACTIVEMQ")),
                    "hostInstanceType": b.get("hostInstanceType").cloned().unwrap_or(json!("")),
                })
            })
            .collect();
        let (page, next) = paginate(&summaries, q);
        let mut out = json!({ "brokerSummaries": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn describe_shared_resources(
        &self,
        ctx: &Ctx,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers(self.auto_settle());
        if !data.brokers.contains_key(id) {
            return Err(not_found(&format!("Can't find requested broker [{id}].")));
        }
        ok(json!({ "sharedResources": [] }))
    }
}

/// Render an ARN-keyed tag map into its wire `tags` object.
fn render_tags(tags: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Object(tags.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
}

/// Render a broker's per-user summary (`[{username, pendingChange?}]`).
fn users_summary(data: &MqData, broker_id: &str) -> Value {
    let arr: Vec<Value> = data
        .users
        .get(broker_id)
        .map(|m| {
            m.iter()
                .map(|(name, u)| {
                    let mut o = json!({ "username": name });
                    if let Some(pc) = u.get("pendingChange").and_then(Value::as_str) {
                        o["pendingChange"] = json!(pc);
                    }
                    o
                })
                .collect()
        })
        .unwrap_or_default();
    Value::Array(arr)
}

// ===================== configurations =====================

impl MqService {
    fn create_configuration(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = b
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let engine = b
            .get("engineType")
            .and_then(Value::as_str)
            .unwrap_or("ACTIVEMQ")
            .to_string();
        let engine_version = b
            .get("engineVersion")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| shared::default_engine_version(&engine).to_string());
        let auth = b
            .get("authenticationStrategy")
            .and_then(Value::as_str)
            .unwrap_or("simple")
            .to_string();
        let id = format!("c-{}", Uuid::new_v4());
        let arn = config_arn(ctx, &id);
        let created = now_iso();
        let rev = json!({ "revision": 1, "created": created, "description": "Auto-generated default for Configuration" });

        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.configurations.insert(
            id.clone(),
            json!({
                "arn": arn,
                "authenticationStrategy": auth,
                "created": created,
                "description": "",
                "engineType": engine,
                "engineVersion": engine_version,
                "id": id,
                "latestRevision": rev,
                "name": name,
            }),
        );
        data.configuration_revisions.insert(
            id.clone(),
            vec![json!({
                "revision": 1,
                "created": created,
                "description": "Auto-generated default for Configuration",
                "data": shared::default_config_data(&engine),
            })],
        );
        // Tags live only in the ARN-keyed tag map (the single source of truth);
        // DescribeConfiguration renders them at read time so later
        // CreateTags/DeleteTags are always reflected.
        let tags = shared::tags_from_body(b);
        if !tags.is_empty() {
            data.tags.insert(arn.clone(), tags);
        }
        ok(json!({
            "arn": arn,
            "authenticationStrategy": auth,
            "created": created,
            "id": id,
            "latestRevision": rev,
            "name": name,
        }))
    }

    fn describe_configuration(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let mut cfg = data
            .configurations
            .get(id)
            .cloned()
            .ok_or_else(|| not_found(&format!("Can't find requested configuration [{id}].")))?;
        // Render tags from the ARN-keyed tag map at read time (no frozen copy).
        if let Some(obj) = cfg.as_object_mut() {
            let arn = obj
                .get("arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            match data.tags.get(&arn).filter(|m| !m.is_empty()) {
                Some(t) => {
                    obj.insert("tags".into(), render_tags(t));
                }
                None => {
                    obj.remove("tags");
                }
            }
        }
        ok(cfg)
    }

    fn describe_configuration_revision(
        &self,
        ctx: &Ctx,
        id: &str,
        rev: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let revs = data
            .configuration_revisions
            .get(id)
            .ok_or_else(|| not_found(&format!("Can't find requested configuration [{id}].")))?;
        let rev_num: i64 = rev
            .parse()
            .map_err(|_| bad_request(&format!("Invalid configuration revision [{rev}].")))?;
        let r = revs
            .iter()
            .find(|r| r.get("revision").and_then(Value::as_i64) == Some(rev_num))
            .ok_or_else(|| not_found(&format!("Can't find requested revision [{rev}].")))?;
        ok(json!({
            "configurationId": id,
            "created": r.get("created").cloned().unwrap_or(json!("")),
            "data": r.get("data").cloned().unwrap_or(json!("")),
            "description": r.get("description").cloned().unwrap_or(json!("")),
        }))
    }

    fn update_configuration(
        &self,
        ctx: &Ctx,
        id: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let data_b64 = b
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let description = b
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.configurations.contains_key(id) {
            return Err(not_found(&format!(
                "Can't find requested configuration [{id}]."
            )));
        }
        let created = now_iso();
        let revs = data
            .configuration_revisions
            .entry(id.to_string())
            .or_default();
        let next_rev = revs
            .iter()
            .filter_map(|r| r.get("revision").and_then(Value::as_i64))
            .max()
            .unwrap_or(0)
            + 1;
        let rev_obj = json!({
            "revision": next_rev,
            "created": created,
            "description": description,
            "data": data_b64,
        });
        revs.push(rev_obj);
        let latest =
            json!({ "revision": next_rev, "created": created, "description": description });
        let cfg = data.configurations.get_mut(id).expect("checked above");
        cfg["latestRevision"] = latest.clone();
        let (arn, name) = (
            cfg.get("arn").cloned().unwrap_or(json!("")),
            cfg.get("name").cloned().unwrap_or(json!("")),
        );
        ok(json!({
            "arn": arn,
            "created": created,
            "id": id,
            "latestRevision": latest,
            "name": name,
            "warnings": [],
        }))
    }

    fn delete_configuration(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.configurations.contains_key(id) {
            return Err(not_found(&format!(
                "Can't find requested configuration [{id}]."
            )));
        }
        // A configuration that any broker currently references (its `current` or
        // staged `pending` configuration) cannot be deleted -- AWS rejects it
        // with ConflictException until the association is removed.
        if config_in_use(data, id) {
            return Err(conflict(&format!(
                "Configuration [{id}] is in use by one or more brokers and cannot be deleted."
            )));
        }
        let cfg = data
            .configurations
            .remove(id)
            .expect("configuration presence checked above");
        data.configuration_revisions.remove(id);
        if let Some(arn) = cfg.get("arn").and_then(Value::as_str) {
            data.tags.remove(arn);
        }
        ok(json!({ "configurationId": id }))
    }

    fn list_configurations(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // ListConfigurations is region-scoped (its ARN embeds the region).
        let items: Vec<Value> = data
            .configurations
            .values()
            .filter(|c| shared::config_region(c) == Some(ctx.region.as_str()))
            .cloned()
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "configurations": page, "maxResults": page_size(q) });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn list_configuration_revisions(
        &self,
        ctx: &Ctx,
        id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let revs = data
            .configuration_revisions
            .get(id)
            .cloned()
            .ok_or_else(|| not_found(&format!("Can't find requested configuration [{id}].")))?;
        let items: Vec<Value> = revs
            .iter()
            .map(|r| {
                json!({
                    "revision": r.get("revision").cloned().unwrap_or(json!(1)),
                    "created": r.get("created").cloned().unwrap_or(json!("")),
                    "description": r.get("description").cloned().unwrap_or(json!("")),
                })
            })
            .collect();
        let (page, next) = paginate(&items, q);
        let mut out =
            json!({ "configurationId": id, "maxResults": page_size(q), "revisions": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

// ===================== users =====================

impl MqService {
    fn create_user(
        &self,
        ctx: &Ctx,
        broker_id: &str,
        username: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.brokers.contains_key(broker_id) {
            return Err(not_found(&format!(
                "Can't find requested broker [{broker_id}]."
            )));
        }
        // RabbitMQ applies user changes immediately; ActiveMQ stages them for
        // the next reboot (matching AWS). So RabbitMQ users are stored current
        // (no pendingChange) and pushed live to the running broker now.
        let is_rabbit = broker_is_rabbit(data, broker_id);
        let users = data.users.entry(broker_id.to_string()).or_default();
        if users.contains_key(username) {
            return Err(conflict(&format!("User [{username}] already exists.")));
        }
        let mut user = json!({
            "username": username,
            "password": b.get("password").cloned().unwrap_or(json!("")),
            "consoleAccess": b.get("consoleAccess").cloned().unwrap_or(json!(false)),
            "groups": b.get("groups").cloned().unwrap_or(json!([])),
            "replicationUser": b.get("replicationUser").cloned().unwrap_or(json!(false)),
        });
        if !is_rabbit {
            user["pendingChange"] = json!("CREATE");
        }
        users.insert(username.to_string(), user.clone());
        if is_rabbit {
            self.spawn_rabbit_apply_user(broker_id.to_string(), &user);
        }
        ok(json!({}))
    }

    /// Push a RabbitMQ user to the running broker via `rabbitmqctl`.
    fn spawn_rabbit_apply_user(&self, broker_id: String, user: &Value) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        let user = BrokerUser {
            username: user
                .get("username")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            password: user
                .get("password")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            groups: user
                .get("groups")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|g| g.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
            console_access: user
                .get("consoleAccess")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        tokio::spawn(async move {
            let _ = runtime.rabbit_apply_user(&broker_id, &user).await;
        });
    }

    /// Remove a RabbitMQ user from the running broker via `rabbitmqctl`.
    fn spawn_rabbit_delete_user(&self, broker_id: String, username: String) {
        let Some(runtime) = self.runtime.clone() else {
            return;
        };
        tokio::spawn(async move {
            let _ = runtime.rabbit_delete_user(&broker_id, &username).await;
        });
    }

    fn describe_user(
        &self,
        ctx: &Ctx,
        broker_id: &str,
        username: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.brokers.contains_key(broker_id) {
            return Err(not_found(&format!(
                "Can't find requested broker [{broker_id}]."
            )));
        }
        let u = data
            .users
            .get(broker_id)
            .and_then(|m| m.get(username))
            .ok_or_else(|| not_found(&format!("Can't find requested user [{username}].")))?;
        let mut out = json!({
            "brokerId": broker_id,
            "username": username,
            "consoleAccess": u.get("consoleAccess").cloned().unwrap_or(json!(false)),
            "groups": u.get("groups").cloned().unwrap_or(json!([])),
            "replicationUser": u.get("replicationUser").cloned().unwrap_or(json!(false)),
        });
        if let Some(pc) = u.get("pendingChange").and_then(Value::as_str) {
            out["pending"] = json!({
                "pendingChange": pc,
                "consoleAccess": u.get("consoleAccess").cloned().unwrap_or(json!(false)),
                "groups": u.get("groups").cloned().unwrap_or(json!([])),
            });
        }
        ok(out)
    }

    fn update_user(
        &self,
        ctx: &Ctx,
        broker_id: &str,
        username: &str,
        b: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.brokers.contains_key(broker_id) {
            return Err(not_found(&format!(
                "Can't find requested broker [{broker_id}]."
            )));
        }
        let is_rabbit = broker_is_rabbit(data, broker_id);
        let u = data
            .users
            .get_mut(broker_id)
            .and_then(|m| m.get_mut(username))
            .ok_or_else(|| not_found(&format!("Can't find requested user [{username}].")))?;
        let obj = u.as_object_mut().expect("user is an object");
        if let Some(v) = b.get("password") {
            obj.insert("password".into(), v.clone());
        }
        if let Some(v) = b.get("consoleAccess") {
            obj.insert("consoleAccess".into(), v.clone());
        }
        if let Some(v) = b.get("groups") {
            obj.insert("groups".into(), v.clone());
        }
        if let Some(v) = b.get("replicationUser") {
            obj.insert("replicationUser".into(), v.clone());
        }
        // RabbitMQ applies the change live now; ActiveMQ stages it for reboot.
        if is_rabbit {
            let user = u.clone();
            self.spawn_rabbit_apply_user(broker_id.to_string(), &user);
        } else {
            u.as_object_mut()
                .expect("user is an object")
                .insert("pendingChange".into(), json!("UPDATE"));
        }
        ok(json!({}))
    }

    fn delete_user(
        &self,
        ctx: &Ctx,
        broker_id: &str,
        username: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.brokers.contains_key(broker_id) {
            return Err(not_found(&format!(
                "Can't find requested broker [{broker_id}]."
            )));
        }
        let is_rabbit = broker_is_rabbit(data, broker_id);
        if is_rabbit {
            // RabbitMQ removes the user immediately; drop it from state and the
            // running broker now.
            if data
                .users
                .get_mut(broker_id)
                .map(|m| m.remove(username).is_some())
                != Some(true)
            {
                return Err(not_found(&format!(
                    "Can't find requested user [{username}]."
                )));
            }
            self.spawn_rabbit_delete_user(broker_id.to_string(), username.to_string());
            return ok(json!({}));
        }
        let u = data
            .users
            .get_mut(broker_id)
            .and_then(|m| m.get_mut(username))
            .ok_or_else(|| not_found(&format!("Can't find requested user [{username}].")))?;
        // ActiveMQ deletion is staged; a reboot removes the user.
        u.as_object_mut()
            .expect("user is an object")
            .insert("pendingChange".into(), json!("DELETE"));
        ok(json!({}))
    }

    fn list_users(
        &self,
        ctx: &Ctx,
        broker_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.brokers.contains_key(broker_id) {
            return Err(not_found(&format!(
                "Can't find requested broker [{broker_id}]."
            )));
        }
        let all = users_summary(data, broker_id);
        let items = all.as_array().cloned().unwrap_or_default();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "brokerId": broker_id, "maxResults": page_size(q), "users": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

// ===================== tags =====================

impl MqService {
    fn create_tags(&self, ctx: &Ctx, arn: &str, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, arn) {
            return Err(not_found(&format!(
                "Can't find requested resource [{arn}]."
            )));
        }
        let entry = data.tags.entry(arn.to_string()).or_default();
        for (k, v) in shared::tags_from_body(b) {
            entry.insert(k, v);
        }
        no_content()
    }

    fn delete_tags(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, arn) {
            return Err(not_found(&format!(
                "Can't find requested resource [{arn}]."
            )));
        }
        if let Some(entry) = data.tags.get_mut(arn) {
            for key in query_all(q, "tagKeys") {
                entry.remove(&key);
            }
        }
        no_content()
    }

    fn list_tags(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !resource_exists(data, arn) {
            return Err(not_found(&format!(
                "Can't find requested resource [{arn}]."
            )));
        }
        let tags: Map<String, Value> = data
            .tags
            .get(arn)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), json!(v))).collect())
            .unwrap_or_default();
        ok(json!({ "tags": Value::Object(tags) }))
    }
}

/// Whether any broker currently references configuration `config_id` (as its
/// `current` or staged `pending` configuration). AWS blocks deleting an in-use
/// configuration.
fn config_in_use(data: &MqData, config_id: &str) -> bool {
    data.brokers.values().any(|b| {
        let configs = b.get("configurations");
        let matches = |slot: &str| {
            configs
                .and_then(|c| c.get(slot))
                .and_then(|c| c.get("id"))
                .and_then(Value::as_str)
                == Some(config_id)
        };
        matches("current") || matches("pending")
    })
}

/// Whether an ARN refers to an existing broker or configuration in this account.
fn resource_exists(data: &MqData, arn: &str) -> bool {
    data.brokers
        .values()
        .any(|b| b.get("brokerArn").and_then(Value::as_str) == Some(arn))
        || data
            .configurations
            .values()
            .any(|c| c.get("arn").and_then(Value::as_str) == Some(arn))
}

// ===================== static metadata =====================

impl MqService {
    fn describe_broker_engine_types(
        &self,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let filter = query_one(q, "engineType");
        let mut types = Vec::new();
        for (engine, versions) in [
            ("ACTIVEMQ", &["5.18", "5.17.6", "5.17.3", "5.16.7"][..]),
            ("RABBITMQ", &["3.13", "3.12.13", "3.11.28", "3.10.25"][..]),
        ] {
            if filter.is_some_and(|f| !f.eq_ignore_ascii_case(engine)) {
                continue;
            }
            types.push(json!({
                "engineType": engine,
                "engineVersions": versions.iter().map(|v| json!({ "name": v })).collect::<Vec<_>>(),
            }));
        }
        let (page, next) = paginate(&types, q);
        let mut out = json!({ "brokerEngineTypes": page, "maxResults": page_size(q) });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn describe_broker_instance_options(
        &self,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine_filter = query_one(q, "engineType");
        let host_filter = query_one(q, "hostInstanceType");
        let storage_filter = query_one(q, "storageType");
        let azs: Vec<Value> = ["us-east-1a", "us-east-1b", "us-east-1c"]
            .iter()
            .map(|n| json!({ "name": n }))
            .collect();
        let mut options = Vec::new();
        for engine in ["ACTIVEMQ", "RABBITMQ"] {
            if engine_filter.is_some_and(|f| !f.eq_ignore_ascii_case(engine)) {
                continue;
            }
            for host in ["mq.t3.micro", "mq.m5.large", "mq.m5.xlarge"] {
                if host_filter.is_some_and(|f| f != host) {
                    continue;
                }
                for storage in ["EBS", "EFS"] {
                    if storage_filter.is_some_and(|f| !f.eq_ignore_ascii_case(storage)) {
                        continue;
                    }
                    let modes = if engine == "RABBITMQ" {
                        json!(["SINGLE_INSTANCE", "CLUSTER_MULTI_AZ"])
                    } else {
                        json!(["SINGLE_INSTANCE", "ACTIVE_STANDBY_MULTI_AZ"])
                    };
                    let versions = if engine == "RABBITMQ" {
                        json!(["3.13", "3.12.13"])
                    } else {
                        json!(["5.18", "5.17.6"])
                    };
                    options.push(json!({
                        "availabilityZones": azs,
                        "engineType": engine,
                        "hostInstanceType": host,
                        "storageType": storage,
                        "supportedDeploymentModes": modes,
                        "supportedEngineVersions": versions,
                    }));
                }
            }
        }
        let (page, next) = paginate(&options, q);
        let mut out = json!({ "brokerInstanceOptions": page, "maxResults": page_size(q) });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> MqService {
        let state = Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "",
        )));
        MqService::new(state)
    }

    fn ctx(region: &str) -> Ctx {
        Ctx {
            account: "123456789012".to_string(),
            region: region.to_string(),
        }
    }

    fn json_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).expect("json response body")
    }

    fn active_body(name: &str) -> Value {
        json!({
            "brokerName": name,
            "engineType": "ACTIVEMQ",
            "deploymentMode": "SINGLE_INSTANCE",
            "hostInstanceType": "mq.m5.large",
            "publiclyAccessible": false
        })
    }

    #[test]
    fn same_broker_name_in_two_regions_both_succeed_and_list_is_region_scoped() {
        let s = svc();
        let east = ctx("us-east-1");
        let west = ctx("us-west-2");

        let e = json_of(
            s.create_broker(&east, &active_body("dup"))
                .expect("east create"),
        );
        let w = json_of(
            s.create_broker(&west, &active_body("dup"))
                .expect("west create"),
        );
        let e_id = e["brokerId"].as_str().unwrap().to_string();
        let w_id = w["brokerId"].as_str().unwrap().to_string();
        assert_ne!(e_id, w_id, "distinct brokers across regions");
        assert!(e["brokerArn"].as_str().unwrap().contains(":us-east-1:"));
        assert!(w["brokerArn"].as_str().unwrap().contains(":us-west-2:"));

        // A same-named create in the same region DOES conflict.
        assert!(s.create_broker(&east, &active_body("dup")).is_err());

        // ListBrokers only shows the current region's broker.
        let listed = json_of(s.list_brokers(&east, &[]).unwrap());
        let ids: Vec<&str> = listed["brokerSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["brokerId"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec![e_id.as_str()], "east list is region-scoped");
    }

    #[test]
    fn create_tags_then_describe_broker_shows_the_tag() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_broker(&c, &active_body("tagged")).unwrap());
        let id = created["brokerId"].as_str().unwrap().to_string();
        let arn = created["brokerArn"].as_str().unwrap().to_string();

        s.create_tags(&c, &arn, &json!({ "tags": { "team": "mq" } }))
            .expect("create tags");

        let mut settled = false;
        let described = json_of(s.describe_broker(&c, &id, &mut settled).unwrap());
        assert_eq!(described["tags"]["team"].as_str(), Some("mq"));
    }

    #[test]
    fn delete_configuration_in_use_by_broker_is_rejected() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_broker(&c, &active_body("withcfg")).unwrap());
        let id = created["brokerId"].as_str().unwrap().to_string();

        let mut settled = false;
        let described = json_of(s.describe_broker(&c, &id, &mut settled).unwrap());
        let cid = described["configurations"]["current"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        match s.delete_configuration(&c, &cid) {
            Err(AwsServiceError::AwsError { status, code, .. }) => {
                assert_eq!(status, StatusCode::CONFLICT);
                assert_eq!(code, "ConflictException");
            }
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("deleting an in-use configuration must be rejected"),
        }
    }

    #[test]
    fn creator_request_id_reused_after_delete_creates_fresh_broker() {
        let s = svc();
        let c = ctx("us-east-1");
        let mut body = active_body("idem");
        body["creatorRequestId"] = json!("token-1");

        let first = json_of(s.create_broker(&c, &body).unwrap());
        let first_id = first["brokerId"].as_str().unwrap().to_string();

        // A repeat before delete is idempotent (same id).
        let repeat = json_of(s.create_broker(&c, &body).unwrap());
        assert_eq!(repeat["brokerId"].as_str().unwrap(), first_id);

        s.delete_broker(&c, &first_id).expect("delete");
        // A describe settles the deletion (removing the broker + creator entry).
        let mut settled = false;
        assert!(s.describe_broker(&c, &first_id, &mut settled).is_err());
        assert!(settled, "delete settle should report a state change");

        // Reusing the same creatorRequestId now creates a brand-new broker with
        // a populated ARN (not the stale deleted id + empty ARN).
        let second = json_of(s.create_broker(&c, &body).unwrap());
        let second_id = second["brokerId"].as_str().unwrap();
        assert_ne!(second_id, first_id, "fresh broker after delete");
        assert!(second["brokerArn"]
            .as_str()
            .unwrap()
            .contains(":broker:idem:"));
    }

    #[test]
    fn activemq_auto_config_description_says_activemq() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_broker(&c, &active_body("descr")).unwrap());
        let id = created["brokerId"].as_str().unwrap().to_string();

        let mut settled = false;
        let described = json_of(s.describe_broker(&c, &id, &mut settled).unwrap());
        let cid = described["configurations"]["current"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let cfg = json_of(s.describe_configuration(&c, &cid).unwrap());
        assert_eq!(
            cfg["latestRevision"]["description"].as_str(),
            Some("Auto-generated default for ActiveMQ")
        );
        let rev = json_of(s.describe_configuration_revision(&c, &cid, "1").unwrap());
        assert_eq!(
            rev["description"].as_str(),
            Some("Auto-generated default for ActiveMQ")
        );
    }

    #[test]
    fn describe_broker_without_transition_does_not_report_settled() {
        let s = svc();
        let c = ctx("us-east-1");
        let created = json_of(s.create_broker(&c, &active_body("stable")).unwrap());
        let id = created["brokerId"].as_str().unwrap().to_string();

        // First describe settles CREATION_IN_PROGRESS -> RUNNING.
        let mut settled = false;
        let _ = s.describe_broker(&c, &id, &mut settled).unwrap();
        assert!(settled, "creation settle fires on first describe");

        // A subsequent describe of a stable broker changes nothing.
        let mut settled2 = true;
        let _ = s.describe_broker(&c, &id, &mut settled2).unwrap();
        assert!(!settled2, "stable read must not report a settle");
    }
}
