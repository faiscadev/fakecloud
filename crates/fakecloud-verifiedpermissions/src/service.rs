//! Verified Permissions (`verifiedpermissions`) awsJson1.0 dispatch + operation
//! handlers.
//!
//! The full 34-operation control plane: policy stores, Cedar schemas, static
//! and template-linked policies, policy templates, identity sources,
//! policy-store aliases, tagging and the four real Cedar authorization
//! operations. State is account-partitioned and persisted; authorization
//! decisions are computed by the `cedar-policy` crate (see [`crate::cedar`]).

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use http::StatusCode;
use rand::Rng;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::cedar;
use crate::persistence::save_snapshot;
use crate::state::{
    SharedVerifiedPermissionsState, StoredAlias, StoredIdentitySource, StoredPolicy,
    StoredPolicyStore, StoredPolicyTemplate, StoredSchema,
};

/// Every operation name in the Verified Permissions Smithy model.
pub const VERIFIEDPERMISSIONS_ACTIONS: &[&str] = &[
    "BatchGetPolicy",
    "BatchIsAuthorized",
    "BatchIsAuthorizedWithToken",
    "CreateIdentitySource",
    "CreatePolicy",
    "CreatePolicyStore",
    "CreatePolicyStoreAlias",
    "CreatePolicyTemplate",
    "DeleteIdentitySource",
    "DeletePolicy",
    "DeletePolicyStore",
    "DeletePolicyStoreAlias",
    "DeletePolicyTemplate",
    "GetIdentitySource",
    "GetPolicy",
    "GetPolicyStore",
    "GetPolicyStoreAlias",
    "GetPolicyTemplate",
    "GetSchema",
    "IsAuthorized",
    "IsAuthorizedWithToken",
    "ListIdentitySources",
    "ListPolicies",
    "ListPolicyStoreAliases",
    "ListPolicyStores",
    "ListPolicyTemplates",
    "ListTagsForResource",
    "PutSchema",
    "TagResource",
    "UntagResource",
    "UpdateIdentitySource",
    "UpdatePolicy",
    "UpdatePolicyStore",
    "UpdatePolicyTemplate",
];

pub struct VerifiedPermissionsService {
    state: SharedVerifiedPermissionsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl VerifiedPermissionsService {
    pub fn new(state: SharedVerifiedPermissionsState) -> Self {
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
impl AwsService for VerifiedPermissionsService {
    fn service_name(&self) -> &str {
        "verifiedpermissions"
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
        VERIFIEDPERMISSIONS_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    action.starts_with("Create")
        || action.starts_with("Update")
        || action.starts_with("Delete")
        || action.starts_with("Put")
        || action == "TagResource"
        || action == "UntagResource"
}

fn dispatch(
    s: &VerifiedPermissionsService,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    match req.action.as_str() {
        "CreatePolicyStore" => s.create_policy_store(req),
        "GetPolicyStore" => s.get_policy_store(req),
        "UpdatePolicyStore" => s.update_policy_store(req),
        "DeletePolicyStore" => s.delete_policy_store(req),
        "ListPolicyStores" => s.list_policy_stores(req),
        "PutSchema" => s.put_schema(req),
        "GetSchema" => s.get_schema(req),
        "CreatePolicy" => s.create_policy(req),
        "GetPolicy" => s.get_policy(req),
        "UpdatePolicy" => s.update_policy(req),
        "DeletePolicy" => s.delete_policy(req),
        "ListPolicies" => s.list_policies(req),
        "BatchGetPolicy" => s.batch_get_policy(req),
        "CreatePolicyTemplate" => s.create_policy_template(req),
        "GetPolicyTemplate" => s.get_policy_template(req),
        "UpdatePolicyTemplate" => s.update_policy_template(req),
        "DeletePolicyTemplate" => s.delete_policy_template(req),
        "ListPolicyTemplates" => s.list_policy_templates(req),
        "CreateIdentitySource" => s.create_identity_source(req),
        "GetIdentitySource" => s.get_identity_source(req),
        "UpdateIdentitySource" => s.update_identity_source(req),
        "DeleteIdentitySource" => s.delete_identity_source(req),
        "ListIdentitySources" => s.list_identity_sources(req),
        "CreatePolicyStoreAlias" => s.create_policy_store_alias(req),
        "GetPolicyStoreAlias" => s.get_policy_store_alias(req),
        "DeletePolicyStoreAlias" => s.delete_policy_store_alias(req),
        "ListPolicyStoreAliases" => s.list_policy_store_aliases(req),
        "IsAuthorized" => s.is_authorized(req),
        "IsAuthorizedWithToken" => s.is_authorized_with_token(req),
        "BatchIsAuthorized" => s.batch_is_authorized(req),
        "BatchIsAuthorizedWithToken" => s.batch_is_authorized_with_token(req),
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
        .map_err(|e| validation(&format!("Request body is malformed: {e}")))
}

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg)
}

fn invalid_state(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_ACCEPTABLE, "InvalidStateException", msg)
}

fn req_str<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation(&format!("{f} must be specified.")))
}

/// Required string field whose `@length` minimum is 0, so the empty string is a
/// valid present value (e.g. `aliasName`).
fn str_field<'a>(b: &'a Value, f: &str) -> Result<&'a str, AwsServiceError> {
    b.get(f)
        .and_then(Value::as_str)
        .ok_or_else(|| validation(&format!("{f} must be specified.")))
}

/// Enum-membership check for a present string field.
fn check_enum(b: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        if !allowed.contains(&s) {
            return Err(validation(&format!(
                "{field} must be one of [{}].",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// Validate the shared pagination inputs: `maxResults` (`@range min 1`) and
/// `nextToken` (`@length 1..=8000`).
fn check_pagination(b: &Value) -> Result<(), AwsServiceError> {
    if let Some(v) = b.get("maxResults") {
        if v.as_i64().unwrap_or(0) < 1 {
            return Err(validation("maxResults must be greater than or equal to 1."));
        }
    }
    if let Some(s) = b.get("nextToken").and_then(Value::as_str) {
        let n = s.chars().count();
        if !(1..=8000).contains(&n) {
            return Err(validation(
                "nextToken must have length between 1 and 8000, inclusive.",
            ));
        }
    }
    Ok(())
}

fn check_len(b: &Value, field: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if let Some(s) = b.get(field).and_then(Value::as_str) {
        let n = s.chars().count();
        if n < min || n > max {
            return Err(validation(&format!(
                "{field} must have length between {min} and {max}, inclusive."
            )));
        }
    }
    Ok(())
}

/// `PolicyStoreId` `@length 1..=200` `@pattern ^[a-zA-Z0-9-/_]*$`.
fn policy_store_id(b: &Value) -> Result<String, AwsServiceError> {
    let s = req_str(b, "policyStoreId")?;
    if s.chars().count() > 200 {
        return Err(validation(
            "policyStoreId must have length between 1 and 200, inclusive.",
        ));
    }
    Ok(s.to_string())
}

/// Random base62 identifier of the given length (matches the shape of real
/// Verified Permissions ids like `C7v5xMplfFH3i3e4Jrzb1a`).
fn gen_id(len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
        .collect()
}

/// RFC3339 / ISO-8601 timestamp (the model marks every timestamp `date-time`).
fn iso(dt: &DateTime<Utc>) -> Value {
    json!(dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

fn policy_store_arn(account: &str, id: &str) -> String {
    format!("arn:aws:verifiedpermissions::{account}:policy-store/{id}")
}

fn alias_arn(region: &str, account: &str, alias_name: &str) -> String {
    format!("arn:aws:verifiedpermissions:{region}:{account}:{alias_name}")
}

/// Extract the policy-store id from a policy-store ARN
/// (`arn:...:policy-store/<id>`).
fn store_id_from_arn(arn: &str) -> Option<String> {
    arn.rsplit_once("policy-store/")
        .map(|(_, id)| id.to_string())
        .filter(|id| !id.is_empty())
}

/// Leading effect keyword of a Cedar statement (`permit`/`forbid`).
fn effect_of(statement: &str) -> Option<&'static str> {
    let t = statement.trim_start();
    if t.starts_with("permit") {
        Some("Permit")
    } else if t.starts_with("forbid") {
        Some("Forbid")
    } else {
        None
    }
}

/// Window an ordered slice of rows by the opaque index `nextToken`.
fn paginate(rows: Vec<Value>, b: &Value) -> (Vec<Value>, Option<String>) {
    let start = b
        .get("nextToken")
        .and_then(Value::as_str)
        .and_then(|t| t.parse::<usize>().ok())
        .unwrap_or(0);
    let max = b
        .get("maxResults")
        .and_then(Value::as_u64)
        .map(|m| m.clamp(1, 1000) as usize)
        .unwrap_or(1000);
    let end = (start + max).min(rows.len());
    let page = rows.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < rows.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

impl VerifiedPermissionsService {
    /// Return `ResourceNotFoundException` if the store does not exist. Used to
    /// order the not-found check ahead of request-content validation so a
    /// missing store surfaces the (declared) not-found error rather than a
    /// validation error the operation does not model.
    fn ensure_store(&self, account: &str, store_id: &str) -> Result<(), AwsServiceError> {
        let guard = self.state.read();
        if guard
            .get(account)
            .map(|a| a.stores.contains_key(store_id))
            .unwrap_or(false)
        {
            Ok(())
        } else {
            Err(not_found(&format!("No policy store with id {store_id}")))
        }
    }

    fn with_store_read<T>(
        &self,
        account: &str,
        store_id: &str,
        f: impl FnOnce(&StoredPolicyStore) -> T,
    ) -> Result<T, AwsServiceError> {
        let guard = self.state.read();
        let store = guard
            .get(account)
            .and_then(|a| a.stores.get(store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        Ok(f(store))
    }

    // ---------- policy stores ----------

    fn create_policy_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        check_len(&b, "description", 0, 150)?;
        check_len(&b, "clientToken", 1, 64)?;
        let mode = b
            .get("validationSettings")
            .and_then(|v| v.get("mode"))
            .and_then(Value::as_str)
            .ok_or_else(|| validation("validationSettings.mode must be specified."))?;
        if mode != "OFF" && mode != "STRICT" {
            return Err(validation(
                "validationSettings.mode must be one of [OFF, STRICT].",
            ));
        }
        let deletion_protection = b
            .get("deletionProtection")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(dp) = &deletion_protection {
            if dp != "ENABLED" && dp != "DISABLED" {
                return Err(validation(
                    "deletionProtection must be one of [ENABLED, DISABLED].",
                ));
            }
        }
        let id = gen_id(22);
        let arn = policy_store_arn(&req.account_id, &id);
        let now = Utc::now();
        let mut tags = std::collections::BTreeMap::new();
        if let Some(m) = b.get("tags").and_then(Value::as_object) {
            for (k, v) in m {
                if let Some(v) = v.as_str() {
                    tags.insert(k.clone(), v.to_string());
                }
            }
        }
        let store = StoredPolicyStore {
            policy_store_id: id.clone(),
            arn: arn.clone(),
            validation_mode: mode.to_string(),
            description: b
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
            deletion_protection,
            created_at: now,
            updated_at: now,
            schema: None,
            policies: Default::default(),
            templates: Default::default(),
            identity_sources: Default::default(),
            tags,
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .stores
            .insert(id.clone(), store);
        ok(json!({
            "policyStoreId": id,
            "arn": arn,
            "createdDate": iso(&now),
            "lastUpdatedDate": iso(&now),
        }))
    }

    fn get_policy_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = policy_store_id(&b)?;
        let include_tags = b.get("tags").and_then(Value::as_bool).unwrap_or(false);
        self.with_store_read(&req.account_id, &id, |store| {
            let mut out = json!({
                "policyStoreId": store.policy_store_id,
                "arn": store.arn,
                "validationSettings": { "mode": store.validation_mode },
                "createdDate": iso(&store.created_at),
                "lastUpdatedDate": iso(&store.updated_at),
            });
            if let Some(d) = &store.description {
                out["description"] = json!(d);
            }
            if let Some(dp) = &store.deletion_protection {
                out["deletionProtection"] = json!(dp);
            }
            if include_tags {
                out["tags"] = json!(store.tags);
            }
            out
        })
        .and_then(ok)
    }

    fn update_policy_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = policy_store_id(&b)?;
        let mode = b
            .get("validationSettings")
            .and_then(|v| v.get("mode"))
            .and_then(Value::as_str)
            .ok_or_else(|| validation("validationSettings.mode must be specified."))?;
        if mode != "OFF" && mode != "STRICT" {
            return Err(validation(
                "validationSettings.mode must be one of [OFF, STRICT].",
            ));
        }
        check_len(&b, "description", 0, 150)?;
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&id))
            .ok_or_else(|| not_found(&format!("No policy store with id {id}")))?;
        store.validation_mode = mode.to_string();
        if let Some(d) = b.get("description").and_then(Value::as_str) {
            store.description = Some(d.to_string());
        }
        if let Some(dp) = b.get("deletionProtection").and_then(Value::as_str) {
            store.deletion_protection = Some(dp.to_string());
        }
        store.updated_at = Utc::now();
        ok(json!({
            "policyStoreId": store.policy_store_id,
            "arn": store.arn,
            "createdDate": iso(&store.created_at),
            "lastUpdatedDate": iso(&store.updated_at),
        }))
    }

    fn delete_policy_store(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = policy_store_id(&b)?;
        let mut guard = self.state.write();
        if let Some(acct) = guard.get_mut(&req.account_id) {
            if let Some(store) = acct.stores.get(&id) {
                if store.deletion_protection.as_deref() == Some("ENABLED") {
                    return Err(invalid_state(
                        "Policy store cannot be deleted because deletion protection is enabled.",
                    ));
                }
                acct.stores.remove(&id);
            }
        }
        ok(json!({}))
    }

    fn list_policy_stores(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        check_pagination(&b)?;
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.stores
                    .values()
                    .map(|s| {
                        let mut item = json!({
                            "policyStoreId": s.policy_store_id,
                            "arn": s.arn,
                            "createdDate": iso(&s.created_at),
                            "lastUpdatedDate": iso(&s.updated_at),
                        });
                        if let Some(d) = &s.description {
                            item["description"] = json!(d);
                        }
                        item
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, &b);
        let mut out = json!({ "policyStores": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        ok(out)
    }

    // ---------- schema ----------

    fn put_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = policy_store_id(&b)?;
        self.ensure_store(&req.account_id, &id)?;
        let cedar_json = b
            .get("definition")
            .and_then(|d| d.get("cedarJson"))
            .and_then(Value::as_str)
            .ok_or_else(|| validation("definition.cedarJson must be specified."))?;
        let parsed: Value = serde_json::from_str(cedar_json)
            .map_err(|e| validation(&format!("definition.cedarJson is not valid JSON: {e}")))?;
        let namespaces: Vec<String> = parsed
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let now = Utc::now();
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&id))
            .ok_or_else(|| not_found(&format!("No policy store with id {id}")))?;
        let created = store.schema.as_ref().map(|s| s.created_at).unwrap_or(now);
        store.schema = Some(StoredSchema {
            cedar_json: cedar_json.to_string(),
            created_at: created,
            updated_at: now,
        });
        ok(json!({
            "policyStoreId": id,
            "namespaces": namespaces,
            "createdDate": iso(&created),
            "lastUpdatedDate": iso(&now),
        }))
    }

    fn get_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let id = policy_store_id(&b)?;
        let out = self.with_store_read(&req.account_id, &id, |store| {
            store.schema.clone().map(|schema| {
                let namespaces: Vec<String> = serde_json::from_str::<Value>(&schema.cedar_json)
                    .ok()
                    .and_then(|v| v.as_object().map(|m| m.keys().cloned().collect()))
                    .unwrap_or_default();
                json!({
                    "policyStoreId": id,
                    "schema": schema.cedar_json,
                    "namespaces": namespaces,
                    "createdDate": iso(&schema.created_at),
                    "lastUpdatedDate": iso(&schema.updated_at),
                })
            })
        })?;
        match out {
            Some(v) => ok(v),
            None => Err(not_found(
                "No schema has been defined for this policy store.",
            )),
        }
    }

    // ---------- policies ----------

    fn create_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        self.ensure_store(&req.account_id, &store_id)?;
        let definition = b
            .get("definition")
            .ok_or_else(|| validation("definition must be specified."))?;
        let name = b.get("name").and_then(Value::as_str).map(str::to_string);

        let (policy, policy_type) = if let Some(st) = definition.get("static") {
            let statement = st
                .get("statement")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| validation("definition.static.statement must be specified."))?;
            if cedar_policy::Policy::parse(None, statement).is_err() {
                return Err(validation(
                    "definition.static.statement is not a valid Cedar policy.",
                ));
            }
            let now = Utc::now();
            (
                StoredPolicy {
                    policy_id: String::new(),
                    policy_type: "STATIC".into(),
                    name: name.clone(),
                    description: st
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    statement: Some(statement.to_string()),
                    template_id: None,
                    principal: None,
                    resource: None,
                    created_at: now,
                    updated_at: now,
                },
                "STATIC",
            )
        } else if let Some(tl) = definition.get("templateLinked") {
            let template_id = tl
                .get("policyTemplateId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    validation("definition.templateLinked.policyTemplateId must be specified.")
                })?
                .to_string();
            let now = Utc::now();
            (
                StoredPolicy {
                    policy_id: String::new(),
                    policy_type: "TEMPLATE_LINKED".into(),
                    name: name.clone(),
                    description: None,
                    statement: None,
                    template_id: Some(template_id),
                    principal: tl.get("principal").cloned(),
                    resource: tl.get("resource").cloned(),
                    created_at: now,
                    updated_at: now,
                },
                "TEMPLATE_LINKED",
            )
        } else {
            return Err(validation(
                "definition must contain either `static` or `templateLinked`.",
            ));
        };

        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        if policy_type == "TEMPLATE_LINKED" {
            let tid = policy.template_id.clone().unwrap_or_default();
            if !store.templates.contains_key(&tid) {
                return Err(not_found(&format!(
                    "No policy template with id {tid} in policy store {store_id}"
                )));
            }
        }
        let policy_id = gen_id(22);
        let mut stored = policy;
        stored.policy_id = policy_id.clone();
        let created = stored.created_at;
        let effect = stored.statement.as_deref().and_then(effect_of);
        let principal = stored.principal.clone();
        let resource = stored.resource.clone();
        store.policies.insert(policy_id.clone(), stored);

        let mut out = json!({
            "policyStoreId": store_id,
            "policyId": policy_id,
            "policyType": policy_type,
            "createdDate": iso(&created),
            "lastUpdatedDate": iso(&created),
        });
        if let Some(e) = effect {
            out["effect"] = json!(e);
        }
        if let Some(p) = principal {
            out["principal"] = p;
        }
        if let Some(r) = resource {
            out["resource"] = r;
        }
        ok(out)
    }

    fn get_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let policy_id = req_str(&b, "policyId")?.to_string();
        let out = self.with_store_read(&req.account_id, &store_id, |store| {
            store.policies.get(&policy_id).map(|p| {
                let mut out = policy_summary(store, p);
                out["definition"] = policy_definition_detail(p);
                out
            })
        })?;
        match out {
            Some(v) => ok(v),
            None => Err(not_found(&format!(
                "No policy with id {policy_id} in policy store {store_id}"
            ))),
        }
    }

    fn update_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        self.ensure_store(&req.account_id, &store_id)?;
        let policy_id = req_str(&b, "policyId")?.to_string();
        let new_statement = b
            .get("definition")
            .and_then(|d| d.get("static"))
            .and_then(|s| s.get("statement"))
            .and_then(Value::as_str);
        if let Some(stmt) = new_statement {
            if cedar_policy::Policy::parse(None, stmt).is_err() {
                return Err(validation(
                    "definition.static.statement is not a valid Cedar policy.",
                ));
            }
        }
        let new_description = b
            .get("definition")
            .and_then(|d| d.get("static"))
            .and_then(|s| s.get("description"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let new_name = b.get("name").and_then(Value::as_str).map(str::to_string);
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        let policy = store
            .policies
            .get_mut(&policy_id)
            .ok_or_else(|| not_found(&format!("No policy with id {policy_id}")))?;
        if let Some(stmt) = new_statement {
            policy.statement = Some(stmt.to_string());
        }
        if new_description.is_some() {
            policy.description = new_description;
        }
        if new_name.is_some() {
            policy.name = new_name;
        }
        policy.updated_at = Utc::now();
        let effect = policy.statement.as_deref().and_then(effect_of);
        let policy_type = policy.policy_type.clone();
        let created = policy.created_at;
        let updated = policy.updated_at;
        let mut out = json!({
            "policyStoreId": store_id,
            "policyId": policy_id,
            "policyType": policy_type,
            "createdDate": iso(&created),
            "lastUpdatedDate": iso(&updated),
        });
        if let Some(e) = effect {
            out["effect"] = json!(e);
        }
        ok(out)
    }

    fn delete_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let policy_id = req_str(&b, "policyId")?.to_string();
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        if store.policies.remove(&policy_id).is_none() {
            return Err(not_found(&format!("No policy with id {policy_id}")));
        }
        ok(json!({}))
    }

    fn list_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        check_pagination(&b)?;
        let filter = b.get("filter");
        let rows = self.with_store_read(&req.account_id, &store_id, |store| {
            store
                .policies
                .values()
                .filter(|p| policy_matches_filter(p, filter))
                .map(|p| {
                    let mut item = policy_summary(store, p);
                    item["definition"] = policy_definition_item(p);
                    item
                })
                .collect::<Vec<_>>()
        })?;
        let (page, next) = paginate(rows, &b);
        let mut out = json!({ "policies": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        ok(out)
    }

    fn batch_get_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let requests = b
            .get("requests")
            .and_then(Value::as_array)
            .ok_or_else(|| validation("requests must be specified."))?;
        let guard = self.state.read();
        let acct = guard.get(&req.account_id);
        let mut results = Vec::new();
        let mut errors = Vec::new();
        for item in requests {
            let sid = item
                .get("policyStoreId")
                .and_then(Value::as_str)
                .unwrap_or("");
            let pid = item.get("policyId").and_then(Value::as_str).unwrap_or("");
            match acct.and_then(|a| a.stores.get(sid)) {
                None => errors.push(json!({
                    "code": "POLICY_STORE_NOT_FOUND",
                    "policyStoreId": sid,
                    "policyId": pid,
                    "message": format!("No policy store with id {sid}"),
                })),
                Some(store) => match store.policies.get(pid) {
                    None => errors.push(json!({
                        "code": "POLICY_NOT_FOUND",
                        "policyStoreId": sid,
                        "policyId": pid,
                        "message": format!("No policy with id {pid}"),
                    })),
                    Some(p) => {
                        let mut out = json!({
                            "policyStoreId": sid,
                            "policyId": pid,
                            "policyType": p.policy_type,
                            "definition": policy_definition_detail(p),
                            "createdDate": iso(&p.created_at),
                            "lastUpdatedDate": iso(&p.updated_at),
                        });
                        if let Some(n) = &p.name {
                            out["name"] = json!(n);
                        }
                        results.push(out);
                    }
                },
            }
        }
        ok(json!({ "results": results, "errors": errors }))
    }

    // ---------- policy templates ----------

    fn create_policy_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let statement = req_str(&b, "statement")?.to_string();
        check_len(&b, "description", 0, 150)?;
        let now = Utc::now();
        let template_id = gen_id(22);
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        store.templates.insert(
            template_id.clone(),
            StoredPolicyTemplate {
                template_id: template_id.clone(),
                name: b.get("name").and_then(Value::as_str).map(str::to_string),
                description: b
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                statement,
                created_at: now,
                updated_at: now,
            },
        );
        ok(json!({
            "policyStoreId": store_id,
            "policyTemplateId": template_id,
            "createdDate": iso(&now),
            "lastUpdatedDate": iso(&now),
        }))
    }

    fn get_policy_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let tid = req_str(&b, "policyTemplateId")?.to_string();
        let out = self.with_store_read(&req.account_id, &store_id, |store| {
            store.templates.get(&tid).map(|t| {
                let mut out = json!({
                    "policyStoreId": store_id,
                    "policyTemplateId": t.template_id,
                    "statement": t.statement,
                    "createdDate": iso(&t.created_at),
                    "lastUpdatedDate": iso(&t.updated_at),
                });
                if let Some(d) = &t.description {
                    out["description"] = json!(d);
                }
                if let Some(n) = &t.name {
                    out["name"] = json!(n);
                }
                out
            })
        })?;
        match out {
            Some(v) => ok(v),
            None => Err(not_found(&format!("No policy template with id {tid}"))),
        }
    }

    fn update_policy_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let tid = req_str(&b, "policyTemplateId")?.to_string();
        let statement = req_str(&b, "statement")?.to_string();
        check_len(&b, "description", 0, 150)?;
        let now = Utc::now();
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        let template = store
            .templates
            .get_mut(&tid)
            .ok_or_else(|| not_found(&format!("No policy template with id {tid}")))?;
        template.statement = statement;
        if let Some(d) = b.get("description").and_then(Value::as_str) {
            template.description = Some(d.to_string());
        }
        if let Some(n) = b.get("name").and_then(Value::as_str) {
            template.name = Some(n.to_string());
        }
        template.updated_at = now;
        ok(json!({
            "policyStoreId": store_id,
            "policyTemplateId": tid,
            "createdDate": iso(&template.created_at),
            "lastUpdatedDate": iso(&now),
        }))
    }

    fn delete_policy_template(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let tid = req_str(&b, "policyTemplateId")?.to_string();
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        if store.templates.remove(&tid).is_none() {
            return Err(not_found(&format!("No policy template with id {tid}")));
        }
        ok(json!({}))
    }

    fn list_policy_templates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        check_pagination(&b)?;
        let rows = self.with_store_read(&req.account_id, &store_id, |store| {
            store
                .templates
                .values()
                .map(|t| {
                    let mut item = json!({
                        "policyStoreId": store_id,
                        "policyTemplateId": t.template_id,
                        "createdDate": iso(&t.created_at),
                        "lastUpdatedDate": iso(&t.updated_at),
                    });
                    if let Some(d) = &t.description {
                        item["description"] = json!(d);
                    }
                    if let Some(n) = &t.name {
                        item["name"] = json!(n);
                    }
                    item
                })
                .collect::<Vec<_>>()
        })?;
        let (page, next) = paginate(rows, &b);
        let mut out = json!({ "policyTemplates": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        ok(out)
    }

    // ---------- identity sources ----------

    fn create_identity_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let configuration = b
            .get("configuration")
            .cloned()
            .ok_or_else(|| validation("configuration must be specified."))?;
        let principal_entity_type = b
            .get("principalEntityType")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| default_principal_entity_type(&configuration));
        let now = Utc::now();
        let identity_source_id = gen_id(22);
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        store.identity_sources.insert(
            identity_source_id.clone(),
            StoredIdentitySource {
                identity_source_id: identity_source_id.clone(),
                configuration,
                principal_entity_type,
                created_at: now,
                updated_at: now,
            },
        );
        ok(json!({
            "createdDate": iso(&now),
            "identitySourceId": identity_source_id,
            "lastUpdatedDate": iso(&now),
            "policyStoreId": store_id,
        }))
    }

    fn get_identity_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let isid = req_str(&b, "identitySourceId")?.to_string();
        let out = self.with_store_read(&req.account_id, &store_id, |store| {
            store.identity_sources.get(&isid).map(|s| {
                json!({
                    "createdDate": iso(&s.created_at),
                    "identitySourceId": s.identity_source_id,
                    "lastUpdatedDate": iso(&s.updated_at),
                    "policyStoreId": store_id,
                    "principalEntityType": s
                        .principal_entity_type
                        .clone()
                        .unwrap_or_else(|| "AWS::Cognito".to_string()),
                    "configuration": s.configuration,
                    "details": identity_source_details(&s.configuration),
                })
            })
        })?;
        match out {
            Some(v) => ok(v),
            None => Err(not_found(&format!("No identity source with id {isid}"))),
        }
    }

    fn update_identity_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let isid = req_str(&b, "identitySourceId")?.to_string();
        let update = b
            .get("updateConfiguration")
            .cloned()
            .ok_or_else(|| validation("updateConfiguration must be specified."))?;
        let now = Utc::now();
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        let source = store
            .identity_sources
            .get_mut(&isid)
            .ok_or_else(|| not_found(&format!("No identity source with id {isid}")))?;
        // An explicit `principalEntityType` wins; otherwise recompute the
        // default from the (possibly new) configuration type — converting an
        // OpenID source to a Cognito one resets the principal entity type to
        // `AWS::Cognito`, matching real Verified Permissions.
        source.principal_entity_type = b
            .get("principalEntityType")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| default_principal_entity_type(&update))
            .or_else(|| source.principal_entity_type.take());
        source.configuration = update;
        source.updated_at = now;
        ok(json!({
            "createdDate": iso(&source.created_at),
            "identitySourceId": isid,
            "lastUpdatedDate": iso(&now),
            "policyStoreId": store_id,
        }))
    }

    fn delete_identity_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let isid = req_str(&b, "identitySourceId")?.to_string();
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No policy store with id {store_id}")))?;
        if store.identity_sources.remove(&isid).is_none() {
            return Err(not_found(&format!("No identity source with id {isid}")));
        }
        ok(json!({}))
    }

    fn list_identity_sources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        check_pagination(&b)?;
        let rows = self.with_store_read(&req.account_id, &store_id, |store| {
            store
                .identity_sources
                .values()
                .map(|s| {
                    json!({
                        "createdDate": iso(&s.created_at),
                        "identitySourceId": s.identity_source_id,
                        "lastUpdatedDate": iso(&s.updated_at),
                        "policyStoreId": store_id,
                        "principalEntityType": s
                            .principal_entity_type
                            .clone()
                            .unwrap_or_else(|| "AWS::Cognito".to_string()),
                        "configuration": s.configuration,
                        "details": identity_source_details(&s.configuration),
                    })
                })
                .collect::<Vec<_>>()
        })?;
        let (page, next) = paginate(rows, &b);
        let mut out = json!({ "identitySources": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        ok(out)
    }

    // ---------- policy-store aliases ----------

    fn create_policy_store_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let alias_name = str_field(&b, "aliasName")?.to_string();
        let store_id = policy_store_id(&b)?;
        let now = Utc::now();
        let arn = alias_arn(&req.region, &req.account_id, &alias_name);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.stores.contains_key(&store_id) {
            return Err(not_found(&format!("No policy store with id {store_id}")));
        }
        if acct.aliases.contains_key(&alias_name) {
            return Err(conflict(&format!(
                "A policy store alias named {alias_name} already exists."
            )));
        }
        acct.aliases.insert(
            alias_name.clone(),
            StoredAlias {
                alias_name: alias_name.clone(),
                policy_store_id: store_id.clone(),
                arn: arn.clone(),
                state: "Active".into(),
                created_at: now,
            },
        );
        ok(json!({
            "aliasName": alias_name,
            "policyStoreId": store_id,
            "aliasArn": arn,
            "createdAt": iso(&now),
        }))
    }

    fn get_policy_store_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let alias_name = str_field(&b, "aliasName")?.to_string();
        let guard = self.state.read();
        let alias = guard
            .get(&req.account_id)
            .and_then(|a| a.aliases.get(&alias_name))
            .ok_or_else(|| not_found(&format!("No policy store alias named {alias_name}")))?;
        ok(json!({
            "aliasName": alias.alias_name,
            "policyStoreId": alias.policy_store_id,
            "aliasArn": alias.arn,
            "createdAt": iso(&alias.created_at),
            "state": alias.state,
        }))
    }

    fn delete_policy_store_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let alias_name = str_field(&b, "aliasName")?.to_string();
        check_len(&b, "aliasName", 0, 150)?;
        check_enum(&b, "deletionMode", &["SoftDelete", "HardDelete"])?;
        if let Some(acct) = self.state.write().get_mut(&req.account_id) {
            acct.aliases.remove(&alias_name);
        }
        ok(json!({}))
    }

    fn list_policy_store_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        check_pagination(&b)?;
        let filter_store = b
            .get("filter")
            .and_then(|f| f.get("policyStoreId"))
            .and_then(Value::as_str);
        let guard = self.state.read();
        let rows: Vec<Value> = guard
            .get(&req.account_id)
            .map(|a| {
                a.aliases
                    .values()
                    .filter(|al| filter_store.is_none_or(|s| al.policy_store_id == s))
                    .map(|al| {
                        json!({
                            "aliasName": al.alias_name,
                            "policyStoreId": al.policy_store_id,
                            "aliasArn": al.arn,
                            "createdAt": iso(&al.created_at),
                            "state": al.state,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(rows, &b);
        let mut out = json!({ "policyStoreAliases": page });
        if let Some(t) = next {
            out["nextToken"] = json!(t);
        }
        ok(out)
    }

    // ---------- authorization (real Cedar) ----------

    fn is_authorized(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        self.ensure_store(&req.account_id, &store_id)?;
        let principal = b
            .get("principal")
            .ok_or_else(|| validation("principal must be specified."))?;
        let action = b
            .get("action")
            .ok_or_else(|| validation("action must be specified."))?;
        let resource = b
            .get("resource")
            .ok_or_else(|| validation("resource must be specified."))?;
        let result = self.with_store_read(&req.account_id, &store_id, |store| {
            cedar::evaluate(
                store,
                principal,
                action,
                resource,
                b.get("context"),
                b.get("entities"),
            )
        })?;
        let r = result.map_err(|e| validation(&e))?;
        ok(authz_response(&r, None))
    }

    fn is_authorized_with_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        self.ensure_store(&req.account_id, &store_id)?;
        let action = b
            .get("action")
            .ok_or_else(|| validation("action must be specified."))?;
        let resource = b
            .get("resource")
            .ok_or_else(|| validation("resource must be specified."))?;
        let token = b
            .get("identityToken")
            .or_else(|| b.get("accessToken"))
            .and_then(Value::as_str)
            .ok_or_else(|| validation("identityToken or accessToken must be specified."))?;
        let (result, principal) = self.with_store_read(&req.account_id, &store_id, |store| {
            let principal = principal_from_token(store, token);
            let r = cedar::evaluate(
                store,
                &principal,
                action,
                resource,
                b.get("context"),
                b.get("entities"),
            );
            (r, principal)
        })?;
        let r = result.map_err(|e| validation(&e))?;
        ok(authz_response(&r, Some(principal)))
    }

    fn batch_is_authorized(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        let requests = b
            .get("requests")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let entities = b.get("entities").cloned();
        let results = self.with_store_read(&req.account_id, &store_id, |store| {
            requests
                .iter()
                .map(|item| {
                    let outcome = batch_eval(
                        store,
                        item.get("principal"),
                        item.get("action"),
                        item.get("resource"),
                        item.get("context"),
                        entities.as_ref(),
                    );
                    batch_result(item.clone(), outcome)
                })
                .collect::<Vec<_>>()
        })?;
        ok(json!({ "results": results }))
    }

    fn batch_is_authorized_with_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let store_id = policy_store_id(&b)?;
        self.ensure_store(&req.account_id, &store_id)?;
        let requests = b
            .get("requests")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let entities = b.get("entities").cloned();
        let token = b
            .get("identityToken")
            .or_else(|| b.get("accessToken"))
            .and_then(Value::as_str)
            .ok_or_else(|| validation("identityToken or accessToken must be specified."))?;
        let (results, principal) = self.with_store_read(&req.account_id, &store_id, |store| {
            let principal = principal_from_token(store, token);
            let results = requests
                .iter()
                .map(|item| {
                    let outcome = batch_eval(
                        store,
                        Some(&principal),
                        item.get("action"),
                        item.get("resource"),
                        item.get("context"),
                        entities.as_ref(),
                    );
                    batch_result(item.clone(), outcome)
                })
                .collect::<Vec<_>>();
            (results, principal)
        })?;
        ok(json!({ "principal": principal, "results": results }))
    }

    // ---------- tagging ----------

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let arn = req_str(&b, "resourceArn")?.to_string();
        let tags = b
            .get("tags")
            .and_then(Value::as_object)
            .ok_or_else(|| validation("tags must be specified."))?;
        let store_id = store_id_from_arn(&arn)
            .ok_or_else(|| not_found(&format!("No resource found for ARN {arn}")))?;
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No resource found for ARN {arn}")))?;
        for (k, v) in tags {
            if let Some(v) = v.as_str() {
                store.tags.insert(k.clone(), v.to_string());
            }
        }
        ok(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let arn = req_str(&b, "resourceArn")?.to_string();
        let keys = b
            .get("tagKeys")
            .and_then(Value::as_array)
            .ok_or_else(|| validation("tagKeys must be specified."))?;
        let store_id = store_id_from_arn(&arn)
            .ok_or_else(|| not_found(&format!("No resource found for ARN {arn}")))?;
        let mut guard = self.state.write();
        let store = guard
            .get_mut(&req.account_id)
            .and_then(|a| a.stores.get_mut(&store_id))
            .ok_or_else(|| not_found(&format!("No resource found for ARN {arn}")))?;
        for k in keys {
            if let Some(k) = k.as_str() {
                store.tags.remove(k);
            }
        }
        ok(json!({}))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let b = parse(req)?;
        let arn = req_str(&b, "resourceArn")?.to_string();
        let store_id = store_id_from_arn(&arn)
            .ok_or_else(|| not_found(&format!("No resource found for ARN {arn}")))?;
        let tags = self.with_store_read(&req.account_id, &store_id, |store| json!(store.tags))?;
        ok(json!({ "tags": tags }))
    }
}

// ===== free helpers =====

/// Summary fields shared by `GetPolicy` / `ListPolicies` / `UpdatePolicy`
/// output (everything but the `definition`, which differs by op).
fn policy_summary(store: &StoredPolicyStore, p: &StoredPolicy) -> Value {
    let mut out = json!({
        "policyStoreId": store.policy_store_id,
        "policyId": p.policy_id,
        "policyType": p.policy_type,
        "createdDate": iso(&p.created_at),
        "lastUpdatedDate": iso(&p.updated_at),
    });
    if let Some(e) = p.statement.as_deref().and_then(effect_of) {
        out["effect"] = json!(e);
    }
    if let Some(n) = &p.name {
        out["name"] = json!(n);
    }
    if let Some(pr) = &p.principal {
        out["principal"] = pr.clone();
    }
    if let Some(r) = &p.resource {
        out["resource"] = r.clone();
    }
    out
}

/// `PolicyDefinitionDetail` for `GetPolicy` / `BatchGetPolicy`.
fn policy_definition_detail(p: &StoredPolicy) -> Value {
    if p.policy_type == "TEMPLATE_LINKED" {
        let mut tl = json!({
            "policyTemplateId": p.template_id.clone().unwrap_or_default(),
        });
        if let Some(pr) = &p.principal {
            tl["principal"] = pr.clone();
        }
        if let Some(r) = &p.resource {
            tl["resource"] = r.clone();
        }
        json!({ "templateLinked": tl })
    } else {
        let mut st = json!({ "statement": p.statement.clone().unwrap_or_default() });
        if let Some(d) = &p.description {
            st["description"] = json!(d);
        }
        json!({ "static": st })
    }
}

/// `PolicyDefinitionItem` for `ListPolicies` (static carries only the
/// description, template-linked carries the template + bindings).
fn policy_definition_item(p: &StoredPolicy) -> Value {
    if p.policy_type == "TEMPLATE_LINKED" {
        let mut tl = json!({
            "policyTemplateId": p.template_id.clone().unwrap_or_default(),
        });
        if let Some(pr) = &p.principal {
            tl["principal"] = pr.clone();
        }
        if let Some(r) = &p.resource {
            tl["resource"] = r.clone();
        }
        json!({ "templateLinked": tl })
    } else {
        let mut st = json!({});
        if let Some(d) = &p.description {
            st["description"] = json!(d);
        }
        json!({ "static": st })
    }
}

/// Apply a `ListPolicies` `PolicyFilter` (principal / resource / policyType /
/// policyTemplateId) to a stored policy.
fn policy_matches_filter(p: &StoredPolicy, filter: Option<&Value>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if let Some(pt) = filter.get("policyType").and_then(Value::as_str) {
        if p.policy_type != pt {
            return false;
        }
    }
    if let Some(tid) = filter.get("policyTemplateId").and_then(Value::as_str) {
        if p.template_id.as_deref() != Some(tid) {
            return false;
        }
    }
    if let Some(pr) = filter.get("principal") {
        if !entity_reference_matches(pr, p.principal.as_ref()) {
            return false;
        }
    }
    if let Some(rs) = filter.get("resource") {
        if !entity_reference_matches(rs, p.resource.as_ref()) {
            return false;
        }
    }
    true
}

/// An `EntityReference` filter matches either any principal/resource
/// (`unspecified`) or a specific `identifier`.
fn entity_reference_matches(reference: &Value, bound: Option<&Value>) -> bool {
    if reference.get("unspecified").is_some() {
        return true;
    }
    match reference.get("identifier") {
        Some(id) => bound == Some(id),
        None => true,
    }
}

/// Build an `IsAuthorized` / `IsAuthorizedWithToken` response body.
fn authz_response(r: &cedar::AuthzResult, principal: Option<Value>) -> Value {
    let determining: Vec<Value> = r
        .determining_policies
        .iter()
        .map(|id| json!({ "policyId": id }))
        .collect();
    let errors: Vec<Value> = r
        .errors
        .iter()
        .map(|e| json!({ "errorDescription": e }))
        .collect();
    let mut out = json!({
        "decision": r.decision,
        "determiningPolicies": determining,
        "errors": errors,
    });
    if let Some(p) = principal {
        out["principal"] = p;
    }
    out
}

/// Result of evaluating one entry of a batch request: either a decision or a
/// request-shaping error.
enum BatchOutcome {
    Ok(cedar::AuthzResult),
    Err(String),
}

fn batch_eval(
    store: &StoredPolicyStore,
    principal: Option<&Value>,
    action: Option<&Value>,
    resource: Option<&Value>,
    context: Option<&Value>,
    entities: Option<&Value>,
) -> BatchOutcome {
    let (Some(principal), Some(action), Some(resource)) = (principal, action, resource) else {
        return BatchOutcome::Err(
            "principal, action and resource are required for each request".to_string(),
        );
    };
    match cedar::evaluate(store, principal, action, resource, context, entities) {
        Ok(r) => BatchOutcome::Ok(r),
        Err(e) => BatchOutcome::Err(e),
    }
}

fn batch_result(request: Value, outcome: BatchOutcome) -> Value {
    match outcome {
        BatchOutcome::Ok(r) => {
            let determining: Vec<Value> = r
                .determining_policies
                .iter()
                .map(|id| json!({ "policyId": id }))
                .collect();
            let errors: Vec<Value> = r
                .errors
                .iter()
                .map(|e| json!({ "errorDescription": e }))
                .collect();
            json!({
                "request": request,
                "decision": r.decision,
                "determiningPolicies": determining,
                "errors": errors,
            })
        }
        BatchOutcome::Err(e) => json!({
            "request": request,
            "decision": "DENY",
            "determiningPolicies": [],
            "errors": [{ "errorDescription": e }],
        }),
    }
}

/// The principal entity type Verified Permissions assigns to an identity source
/// when the caller does not specify one explicitly. A Cognito user-pool source
/// defaults its principal to the built-in `AWS::Cognito` entity type; an OpenID
/// Connect source has no server-side default (the caller supplies it), so this
/// returns `None` and the stored value is left to the explicit request field.
fn default_principal_entity_type(configuration: &Value) -> Option<String> {
    if configuration.get("cognitoUserPoolConfiguration").is_some() {
        Some("AWS::Cognito".to_string())
    } else {
        None
    }
}

/// `IdentitySourceDetails` view (deprecated flat view kept for compatibility)
/// derived from a Cognito `Configuration`.
fn identity_source_details(configuration: &Value) -> Value {
    let mut details = Map::new();
    if let Some(c) = configuration.get("cognitoUserPoolConfiguration") {
        if let Some(arn) = c.get("userPoolArn").and_then(Value::as_str) {
            details.insert("userPoolArn".into(), json!(arn));
        }
        if let Some(ids) = c.get("clientIds") {
            details.insert("clientIds".into(), ids.clone());
        }
        details.insert("openIdIssuer".into(), json!("COGNITO"));
    }
    if let Some(c) = configuration.get("openIdConnectConfiguration") {
        if let Some(url) = c.get("issuer").and_then(Value::as_str) {
            details.insert("discoveryUrl".into(), json!(url));
        }
    }
    Value::Object(details)
}

/// Resolve the principal `EntityIdentifier` from a bearer JWT, using the store's
/// single identity source (if any) to pick the principal entity type. The
/// principal id is the token's `sub` claim (falling back to
/// `cognito:username`); when the token can't be decoded a deterministic
/// placeholder is used so authorization still evaluates.
fn principal_from_token(store: &StoredPolicyStore, token: &str) -> Value {
    let claims = decode_jwt_claims(token).unwrap_or_default();
    let entity_id = claims
        .get("sub")
        .and_then(Value::as_str)
        .or_else(|| claims.get("cognito:username").and_then(Value::as_str))
        .unwrap_or("unknown")
        .to_string();
    let entity_type = store
        .identity_sources
        .values()
        .next()
        .and_then(|s| s.principal_entity_type.clone())
        .unwrap_or_else(|| "AWS::Cognito::User".to_string());
    json!({ "entityType": entity_type, "entityId": entity_id })
}

/// Decode the (unverified) claim set from a JWT's payload segment.
fn decode_jwt_claims(token: &str) -> Option<Map<String, Value>> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|v| v.as_object().cloned())
}

#[cfg(test)]
mod tests;
