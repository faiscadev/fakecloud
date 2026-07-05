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
use crate::state::{MqData, SharedMqState};

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
    // DescribeBroker settles in-flight lifecycle transitions, so persist too.
    "DescribeBroker",
];

pub struct MqService {
    state: SharedMqState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl MqService {
    pub fn new(state: SharedMqState) -> Self {
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
        let result = self.dispatch(action, &labels, &req);
        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
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
    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req)?;
        crate::validate::validate_input(action, &body)?;
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        crate::validate::validate_query(&q)?;
        let a0 = labels.first().map(String::as_str).unwrap_or_default();
        let a1 = labels.get(1).map(String::as_str).unwrap_or_default();
        match action {
            "CreateBroker" => self.create_broker(&ctx, &body),
            "DescribeBroker" => self.describe_broker(&ctx, a0),
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
        }
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
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn broker_arn(ctx: &Ctx, name: &str, id: &str) -> String {
    format!(
        "arn:aws:mq:{}:{}:broker:{}:{}",
        ctx.region, ctx.account, name, id
    )
}

fn config_arn(ctx: &Ctx, id: &str) -> String {
    format!(
        "arn:aws:mq:{}:{}:configuration:{}",
        ctx.region, ctx.account, id
    )
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

/// Whether an engine string names RabbitMQ, case-insensitively. Amazon MQ
/// echoes back the engine type in the display case the caller sent
/// (`ActiveMQ` / `RabbitMQ`), so all internal branching is case-insensitive.
fn is_rabbit(engine: &str) -> bool {
    engine.eq_ignore_ascii_case("RABBITMQ")
}

/// FNV-1a hash for deterministic synthesis of a broker instance IP.
fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Convert a `Tags` map member of a request body into a `BTreeMap`.
fn tags_from_body(b: &Value) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(obj) = b.get("tags").and_then(Value::as_object) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// Paginate a slice by `maxResults` (default 100) + numeric `nextToken` offset.
/// Returns the page plus the next token (an offset), if more remain.
fn paginate(items: &[Value], q: &[(String, String)]) -> (Vec<Value>, Option<String>) {
    let max = query_one(q, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(100);
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

/// Derive a broker's `brokerInstances` (per-engine wire endpoints + console URL
/// + IP) from live state. Only present once the broker is `RUNNING`.
fn broker_instances(id: &str, engine: &str, region: &str, deployment_mode: &str) -> Value {
    let count = if !is_rabbit(engine) && deployment_mode == "ACTIVE_STANDBY_MULTI_AZ" {
        2
    } else {
        1
    };
    let mut out = Vec::new();
    for i in 1..=count {
        let ip = {
            let h = hash_str(&format!("{id}-{i}"));
            format!("10.{}.{}.{}", (h >> 16) & 0xff, (h >> 8) & 0xff, h & 0xff)
        };
        if is_rabbit(engine) {
            let host = format!("{id}.mq.{region}.amazonaws.com");
            out.push(json!({
                "consoleURL": format!("https://{host}"),
                "endpoints": [format!("amqps://{host}:5671")],
                "ipAddress": ip,
            }));
        } else {
            let host = format!("{id}-{i}.mq.{region}.amazonaws.com");
            out.push(json!({
                "consoleURL": format!("https://{host}:8162"),
                "endpoints": [
                    format!("ssl://{host}:61617"),
                    format!("amqp+ssl://{host}:5671"),
                    format!("stomp+ssl://{host}:61614"),
                    format!("mqtt+ssl://{host}:8883"),
                    format!("wss://{host}:61619"),
                ],
                "ipAddress": ip,
            }));
        }
    }
    Value::Array(out)
}

/// The default configuration `Data` for an engine, base64-encoded (matching
/// AWS's auto-generated broker configuration).
fn default_config_data(engine: &str) -> String {
    let xml = if is_rabbit(engine) {
        "consumer_timeout = 1800000\n".to_string()
    } else {
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\
         <broker xmlns=\"http://activemq.apache.org/schema/core\" start=\"false\">\n\
         </broker>\n"
            .to_string()
    };
    base64::engine::general_purpose::STANDARD.encode(xml.as_bytes())
}

impl MqService {
    /// Look up a broker, first settling any in-flight lifecycle transition.
    /// Returns the (possibly reconciled) live broker object, or `None` if it
    /// does not exist / finished deleting.
    fn broker_snapshot(data: &mut MqData, id: &str) -> Option<Value> {
        data.reconcile_brokers();
        data.brokers.get(id).cloned()
    }
}

// ===================== brokers =====================

impl MqService {
    fn create_broker(&self, ctx: &Ctx, b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = b
            .get("brokerName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let engine = b
            .get("engineType")
            .and_then(Value::as_str)
            .unwrap_or("ACTIVEMQ")
            .to_string();
        let deployment = b
            .get("deploymentMode")
            .and_then(Value::as_str)
            .unwrap_or("SINGLE_INSTANCE")
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
        // Broker names are unique within an account+region.
        if data
            .brokers
            .values()
            .any(|x| x.get("brokerName").and_then(Value::as_str) == Some(name.as_str()))
        {
            return Err(conflict(&format!(
                "Broker name {name} already exists. Please choose a different name."
            )));
        }

        let id = format!("b-{}", Uuid::new_v4());
        let arn = broker_arn(ctx, &name, &id);
        let engine_version = b
            .get("engineVersion")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| default_engine_version(&engine).to_string());
        // AWS returns the authentication strategy lower-cased (`simple`); the
        // default when the caller omits it is `simple`.
        let auth = b
            .get("authenticationStrategy")
            .and_then(Value::as_str)
            .unwrap_or("simple")
            .to_string();

        let mut broker = Map::new();
        broker.insert("brokerId".into(), json!(id));
        broker.insert("brokerArn".into(), json!(arn));
        broker.insert("brokerName".into(), json!(name));
        broker.insert("brokerState".into(), json!("CREATION_IN_PROGRESS"));
        broker.insert("engineType".into(), json!(engine));
        broker.insert("engineVersion".into(), json!(engine_version));
        broker.insert("authenticationStrategy".into(), json!(auth));
        broker.insert("created".into(), json!(now_iso()));
        broker.insert("deploymentMode".into(), json!(deployment));
        broker.insert(
            "hostInstanceType".into(),
            b.get("hostInstanceType")
                .cloned()
                .unwrap_or(json!("mq.m5.large")),
        );
        broker.insert(
            "publiclyAccessible".into(),
            b.get("publiclyAccessible").cloned().unwrap_or(json!(false)),
        );
        broker.insert(
            "autoMinorVersionUpgrade".into(),
            b.get("autoMinorVersionUpgrade")
                .cloned()
                .unwrap_or(json!(false)),
        );
        // AWS returns the storage type lower-cased (`ebs` / `efs`).
        broker.insert(
            "storageType".into(),
            b.get("storageType").cloned().unwrap_or(json!("ebs")),
        );
        broker.insert(
            "securityGroups".into(),
            b.get("securityGroups").cloned().unwrap_or(json!([])),
        );
        // When no subnets are supplied, AWS places the broker in subnets of the
        // account's default VPC: one for SINGLE_INSTANCE, two for the multi-AZ
        // deployment modes. Synthesize deterministic ids so DescribeBroker
        // returns a stable, non-empty list (Terraform's `subnet_ids` is
        // Computed and must round-trip through import).
        let subnets = match b.get("subnetIds").and_then(Value::as_array) {
            Some(a) if !a.is_empty() => Value::Array(a.clone()),
            _ => {
                let n = if deployment == "SINGLE_INSTANCE" {
                    1
                } else {
                    2
                };
                Value::Array(
                    (0..n)
                        .map(|i| {
                            let h = hash_str(&format!("{id}-subnet-{i}"));
                            json!(format!("subnet-{h:017x}"))
                        })
                        .collect(),
                )
            }
        };
        broker.insert("subnetIds".into(), subnets);
        if let Some(m) = b.get("maintenanceWindowStartTime") {
            broker.insert("maintenanceWindowStartTime".into(), m.clone());
        } else {
            broker.insert(
                "maintenanceWindowStartTime".into(),
                json!({ "dayOfWeek": "SUNDAY", "timeOfDay": "00:00", "timeZone": "UTC" }),
            );
        }
        // EncryptionOptions is always present in DescribeBroker; the default is
        // an AWS-owned key.
        broker.insert(
            "encryptionOptions".into(),
            b.get("encryptionOptions")
                .cloned()
                .unwrap_or_else(|| json!({ "useAwsOwnedKey": true })),
        );
        let audit = b
            .get("logs")
            .and_then(|l| l.get("audit"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let general = b
            .get("logs")
            .and_then(|l| l.get("general"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut logs = json!({
            "audit": audit,
            "general": general,
            "generalLogGroup": format!("/aws/amazonmq/broker/{id}/general"),
        });
        if audit {
            logs["auditLogGroup"] = json!(format!("/aws/amazonmq/broker/{id}/audit"));
        }
        broker.insert("logs".into(), logs);
        let tags = tags_from_body(b);
        if !tags.is_empty() {
            broker.insert(
                "tags".into(),
                Value::Object(tags.iter().map(|(k, v)| (k.clone(), json!(v))).collect()),
            );
        }
        broker.insert(
            "dataReplicationMode".into(),
            b.get("dataReplicationMode")
                .cloned()
                .unwrap_or(json!("NONE")),
        );

        // Users supplied inline at create time become the broker's current users.
        let mut user_map = std::collections::BTreeMap::new();
        if let Some(arr) = b.get("users").and_then(Value::as_array) {
            for u in arr {
                if let Some(username) = u.get("username").and_then(Value::as_str) {
                    user_map.insert(username.to_string(), normalize_user(u));
                }
            }
        }

        // ActiveMQ brokers are backed by a configuration; RabbitMQ has none.
        if !is_rabbit(&engine) {
            let cfg = if let Some(c) = b.get("configuration") {
                json!({ "id": c.get("id").cloned().unwrap_or(json!("")), "revision": c.get("revision").cloned().unwrap_or(json!(1)) })
            } else {
                let cid = format!("c-{}", Uuid::new_v4());
                let created = now_iso();
                let rev = json!({ "revision": 1, "created": created, "description": "Auto-generated default for RabbitMQ" });
                data.configurations.insert(
                    cid.clone(),
                    json!({
                        "arn": config_arn(ctx, &cid),
                        "authenticationStrategy": auth,
                        "created": created,
                        "description": "Auto-generated default for ActiveMQ",
                        "engineType": engine,
                        "engineVersion": engine_version,
                        "id": cid,
                        "latestRevision": rev,
                        "name": format!("{name}-configuration"),
                    }),
                );
                data.configuration_revisions.insert(
                    cid.clone(),
                    vec![json!({
                        "revision": 1,
                        "created": created,
                        "description": "Auto-generated default for ActiveMQ",
                        "data": default_config_data(&engine),
                    })],
                );
                json!({ "id": cid, "revision": 1 })
            };
            broker.insert(
                "configurations".into(),
                json!({ "current": cfg, "history": [] }),
            );
        }

        data.brokers.insert(id.clone(), Value::Object(broker));
        data.users.insert(id.clone(), user_map);
        if let Some(cr) = creator {
            data.broker_creator_ids.insert(cr, id.clone());
        }
        if !tags.is_empty() {
            data.tags.insert(arn.clone(), tags);
        }

        ok(json!({ "brokerId": id, "brokerArn": arn }))
    }

    fn describe_broker(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let mut broker = Self::broker_snapshot(data, id)
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
        // brokerInstances are served once the broker is running.
        if state == "RUNNING" {
            obj.insert(
                "brokerInstances".into(),
                broker_instances(id, &engine, &ctx.region, &deployment),
            );
        }
        // users summary is derived from live per-broker user state.
        obj.insert("users".into(), users_summary(data, id));
        obj.insert("actionsRequired".into(), json!([]));
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
        data.reconcile_brokers();
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
        data.reconcile_brokers();
        let broker = data
            .brokers
            .get_mut(id)
            .ok_or_else(|| not_found(&format!("Can't find requested broker [{id}].")))?;
        broker
            .as_object_mut()
            .expect("broker is an object")
            .insert("brokerState".into(), json!("DELETION_IN_PROGRESS"));
        ok(json!({ "brokerId": id }))
    }

    fn reboot_broker(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers();
        let broker = data
            .brokers
            .get_mut(id)
            .ok_or_else(|| not_found(&format!("Can't find requested broker [{id}].")))?;
        broker
            .as_object_mut()
            .expect("broker is an object")
            .insert("brokerState".into(), json!("REBOOT_IN_PROGRESS"));
        ok(json!({}))
    }

    fn promote(&self, ctx: &Ctx, id: &str, _b: &Value) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.reconcile_brokers();
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
        data.reconcile_brokers();
        let summaries: Vec<Value> = data
            .brokers
            .values()
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
        data.reconcile_brokers();
        if !data.brokers.contains_key(id) {
            return Err(not_found(&format!("Can't find requested broker [{id}].")));
        }
        ok(json!({ "sharedResources": [] }))
    }
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

/// Normalize a create-broker inline user into the stored user object.
fn normalize_user(u: &Value) -> Value {
    json!({
        "username": u.get("username").cloned().unwrap_or(json!("")),
        "password": u.get("password").cloned().unwrap_or(json!("")),
        "consoleAccess": u.get("consoleAccess").cloned().unwrap_or(json!(false)),
        "groups": u.get("groups").cloned().unwrap_or(json!([])),
        "replicationUser": u.get("replicationUser").cloned().unwrap_or(json!(false)),
    })
}

fn default_engine_version(engine: &str) -> &'static str {
    if is_rabbit(engine) {
        "3.13"
    } else {
        "5.18"
    }
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
            .unwrap_or_else(|| default_engine_version(&engine).to_string());
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
                "data": default_config_data(&engine),
            })],
        );
        let tags = tags_from_body(b);
        if !tags.is_empty() {
            data.tags.insert(arn.clone(), tags);
            if let Some(c) = data.configurations.get_mut(&id) {
                c["tags"] = Value::Object(
                    tags_from_body(b)
                        .iter()
                        .map(|(k, v)| (k.clone(), json!(v)))
                        .collect(),
                );
            }
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
        let cfg = data
            .configurations
            .get(id)
            .cloned()
            .ok_or_else(|| not_found(&format!("Can't find requested configuration [{id}].")))?;
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
        let cfg = data
            .configurations
            .remove(id)
            .ok_or_else(|| not_found(&format!("Can't find requested configuration [{id}].")))?;
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
        let items: Vec<Value> = data.configurations.values().cloned().collect();
        let (page, next) = paginate(&items, q);
        let mut out = json!({ "configurations": page, "maxResults": 100 });
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
        let mut out = json!({ "configurationId": id, "maxResults": 100, "revisions": page });
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
        let users = data.users.entry(broker_id.to_string()).or_default();
        if users.contains_key(username) {
            return Err(conflict(&format!("User [{username}] already exists.")));
        }
        users.insert(
            username.to_string(),
            json!({
                "username": username,
                "password": b.get("password").cloned().unwrap_or(json!("")),
                "consoleAccess": b.get("consoleAccess").cloned().unwrap_or(json!(false)),
                "groups": b.get("groups").cloned().unwrap_or(json!([])),
                "replicationUser": b.get("replicationUser").cloned().unwrap_or(json!(false)),
                "pendingChange": "CREATE",
            }),
        );
        ok(json!({}))
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
        obj.insert("pendingChange".into(), json!("UPDATE"));
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
        let u = data
            .users
            .get_mut(broker_id)
            .and_then(|m| m.get_mut(username))
            .ok_or_else(|| not_found(&format!("Can't find requested user [{username}].")))?;
        // Deletion is staged; a reboot removes the user.
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
        let mut out = json!({ "brokerId": broker_id, "maxResults": 100, "users": page });
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
        for (k, v) in tags_from_body(b) {
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
        ok(json!({ "brokerEngineTypes": types, "maxResults": 100 }))
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
        ok(json!({ "brokerInstanceOptions": options, "maxResults": 100 }))
    }
}
