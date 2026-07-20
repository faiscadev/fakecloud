//! AWS Lake Formation restJson1 service handler.
//!
//! Implements the Lake Formation governance control plane over Glue: LF-tags
//! and LF-tag expressions, fine-grained permission grants, registered
//! data-lake resources, data-lake settings, data-cell filters, opt-ins,
//! governed-table transactions + object lists, Identity Center configuration,
//! storage optimizers, resource-attached LF-tags, and temporary-credential
//! vending. There is no backing Glue catalogue or query engine, so credential
//! vending and query-planning ops return coherent synthetic values while
//! everything else is real, persisted, account-partitioned CRUD.
//!
//! Every operation is a single-segment restJson1 `POST /<OperationName>` with a
//! JSON body (no `@httpLabel`/`@httpQuery`/`@httpPayload` members), so routing
//! keys purely on the path segment.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use http::StatusCode;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    ckey, GrantRecord, IdentityCenterRecord, LFTagExpressionRecord, LFTagRecord, OptInRecord,
    QueryRecord, ResourceInfoRecord, SharedLakeFormationState, TransactionRecord,
};
use crate::validate;

pub const LAKEFORMATION_ACTIONS: &[&str] = &[
    "AddLFTagsToResource",
    "AssumeDecoratedRoleWithSAML",
    "BatchGrantPermissions",
    "BatchRevokePermissions",
    "CancelTransaction",
    "CommitTransaction",
    "CreateDataCellsFilter",
    "CreateLFTag",
    "CreateLFTagExpression",
    "CreateLakeFormationIdentityCenterConfiguration",
    "CreateLakeFormationOptIn",
    "DeleteDataCellsFilter",
    "DeleteLFTag",
    "DeleteLFTagExpression",
    "DeleteLakeFormationIdentityCenterConfiguration",
    "DeleteLakeFormationOptIn",
    "DeleteObjectsOnCancel",
    "DeregisterResource",
    "DescribeLakeFormationIdentityCenterConfiguration",
    "DescribeResource",
    "DescribeTransaction",
    "ExtendTransaction",
    "GetDataCellsFilter",
    "GetDataLakePrincipal",
    "GetDataLakeSettings",
    "GetEffectivePermissionsForPath",
    "GetLFTag",
    "GetLFTagExpression",
    "GetQueryState",
    "GetQueryStatistics",
    "GetResourceLFTags",
    "GetTableObjects",
    "GetTemporaryDataLocationCredentials",
    "GetTemporaryGluePartitionCredentials",
    "GetTemporaryGlueTableCredentials",
    "GetWorkUnitResults",
    "GetWorkUnits",
    "GrantPermissions",
    "ListDataCellsFilter",
    "ListLFTagExpressions",
    "ListLFTags",
    "ListLakeFormationOptIns",
    "ListPermissions",
    "ListResources",
    "ListTableStorageOptimizers",
    "ListTransactions",
    "PutDataLakeSettings",
    "RegisterResource",
    "RemoveLFTagsFromResource",
    "RevokePermissions",
    "SearchDatabasesByLFTags",
    "SearchTablesByLFTags",
    "StartQueryPlanning",
    "StartTransaction",
    "UpdateDataCellsFilter",
    "UpdateLFTag",
    "UpdateLFTagExpression",
    "UpdateLakeFormationIdentityCenterConfiguration",
    "UpdateResource",
    "UpdateTableObjects",
    "UpdateTableStorageOptimizer",
];

/// Actions that mutate persisted state and therefore trigger a snapshot.
const MUTATING: &[&str] = &[
    "AddLFTagsToResource",
    "BatchGrantPermissions",
    "BatchRevokePermissions",
    "CancelTransaction",
    "CommitTransaction",
    "CreateDataCellsFilter",
    "CreateLFTag",
    "CreateLFTagExpression",
    "CreateLakeFormationIdentityCenterConfiguration",
    "CreateLakeFormationOptIn",
    "DeleteDataCellsFilter",
    "DeleteLFTag",
    "DeleteLFTagExpression",
    "DeleteLakeFormationIdentityCenterConfiguration",
    "DeleteLakeFormationOptIn",
    "DeregisterResource",
    "ExtendTransaction",
    "GrantPermissions",
    "PutDataLakeSettings",
    "RegisterResource",
    "RemoveLFTagsFromResource",
    "RevokePermissions",
    "StartQueryPlanning",
    "StartTransaction",
    "UpdateDataCellsFilter",
    "UpdateLFTag",
    "UpdateLFTagExpression",
    "UpdateLakeFormationIdentityCenterConfiguration",
    "UpdateResource",
    "UpdateTableObjects",
    "UpdateTableStorageOptimizer",
];

pub struct LakeFormationService {
    state: SharedLakeFormationState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl LakeFormationService {
    pub fn new(state: SharedLakeFormationState) -> Self {
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

    async fn save_snapshot(&self) {
        crate::persistence::save_snapshot(
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

    /// Route a request to an action name by its single path segment.
    fn resolve_action(req: &AwsRequest) -> Option<&'static str> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let seg = raw.strip_prefix('/').unwrap_or(raw);
        let seg = seg.strip_suffix('/').unwrap_or(seg);
        LAKEFORMATION_ACTIONS.iter().copied().find(|a| *a == seg)
    }
}

#[async_trait]
impl AwsService for LakeFormationService {
    fn service_name(&self) -> &str {
        "lakeformation"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let result = self.dispatch(action, &req);

        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        LAKEFORMATION_ACTIONS
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

fn ok(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

fn gen_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn err(status: StatusCode, code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, msg)
}
fn entity_not_found(msg: impl Into<String>) -> AwsServiceError {
    err(StatusCode::BAD_REQUEST, "EntityNotFoundException", msg)
}
fn invalid_input(msg: impl Into<String>) -> AwsServiceError {
    err(StatusCode::BAD_REQUEST, "InvalidInputException", msg)
}
fn already_exists(msg: impl Into<String>) -> AwsServiceError {
    err(StatusCode::BAD_REQUEST, "AlreadyExistsException", msg)
}

fn req_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn str_field(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

fn string_list(body: &Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The effective catalog id: the request's `CatalogId` or the caller account.
fn catalog_id(body: &Value, account_id: &str) -> String {
    str_field(body, "CatalogId").unwrap_or_else(|| account_id.to_string())
}

// ---------------------------------------------------------------------------
// Synthetic temporary credentials
// ---------------------------------------------------------------------------

fn alphanum(len: usize) -> String {
    let raw = format!("{}{}{}", gen_uuid(), gen_uuid(), gen_uuid());
    raw.replace('-', "")
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(len)
        .collect()
}

/// A synthetic but well-formed set of STS-style temporary credentials, mirroring
/// the `FSIA`-prefixed key / 40-char secret / long session-token shape used by
/// fakecloud's other credential-vending surfaces.
struct TempCreds {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    expiration: DateTime<Utc>,
}

fn vend_credentials(duration_seconds: Option<i64>) -> TempCreds {
    let dur = duration_seconds.unwrap_or(3600).clamp(900, 43200);
    TempCreds {
        access_key_id: format!("FSIA{}", alphanum(16)),
        secret_access_key: alphanum(40),
        session_token: format!("FQoGZXIvYXdzE{}", alphanum(320)),
        expiration: Utc::now() + Duration::seconds(dur),
    }
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

/// Token/limit pagination reading `MaxResults`/`NextToken` from the JSON body.
fn page_response(
    items: Vec<Value>,
    list_key: &str,
    body: &Value,
    extra: &[(&str, Value)],
) -> Result<AwsResponse, AwsServiceError> {
    let max = body
        .get("MaxResults")
        .and_then(serde_json::Value::as_u64)
        .map(|n| n as usize)
        .filter(|n| *n > 0)
        .unwrap_or(1000);
    let token = str_field(body, "NextToken");
    let (page, next) = paginate_checked(&items, token.as_deref(), max)
        .map_err(|_| invalid_input("Invalid NextToken"))?;
    let mut out = Map::new();
    out.insert(list_key.to_string(), Value::Array(page));
    if let Some(t) = next {
        out.insert("NextToken".to_string(), Value::String(t));
    }
    for (k, v) in extra {
        out.insert((*k).to_string(), v.clone());
    }
    Ok(ok(Value::Object(out)))
}

// ---------------------------------------------------------------------------
// Resource signature (for resource-attached LF-tags)
// ---------------------------------------------------------------------------

/// A stable string signature for a `Resource` union value, used to identify a
/// resource for grant/revoke matching, opt-in matching, and resource-attached
/// LF-tag keying. The signature captures the resource's full logical identity:
/// the catalog id (defaulted to `account_id` when omitted, so the two encodings
/// of the caller's own catalog collapse to one), the database/table names, and
/// — for a `TableWithColumns` — the (sorted) column set, so column-scoped
/// grants on the same table stay distinct.
fn resource_signature(resource: &Value, account_id: &str) -> String {
    let cat = |v: &Value| {
        v.get("CatalogId")
            .and_then(Value::as_str)
            .unwrap_or(account_id)
            .to_string()
    };
    if let Some(db) = resource.get("Database") {
        return format!(
            "db\u{1}{}\u{1}{}",
            cat(db),
            db.get("Name").and_then(Value::as_str).unwrap_or("")
        );
    }
    if let Some(t) = resource.get("Table") {
        let wildcard = t.get("TableWildcard").is_some();
        return format!(
            "table\u{1}{}\u{1}{}\u{1}{}\u{1}{}",
            cat(t),
            t.get("DatabaseName").and_then(Value::as_str).unwrap_or(""),
            t.get("Name").and_then(Value::as_str).unwrap_or(""),
            wildcard
        );
    }
    if let Some(t) = resource.get("TableWithColumns") {
        let mut cols: Vec<String> = t
            .get("ColumnNames")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|c| c.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        cols.sort();
        let wildcard = serde_json::to_string(t.get("ColumnWildcard").unwrap_or(&Value::Null))
            .unwrap_or_default();
        return format!(
            "twc\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}",
            cat(t),
            t.get("DatabaseName").and_then(Value::as_str).unwrap_or(""),
            t.get("Name").and_then(Value::as_str).unwrap_or(""),
            cols.join(","),
            wildcard
        );
    }
    if let Some(c) = resource.get("Catalog") {
        return format!(
            "catalog\u{1}{}",
            c.get("Id").and_then(Value::as_str).unwrap_or(account_id)
        );
    }
    if let Some(dl) = resource.get("DataLocation") {
        return format!(
            "dl\u{1}{}\u{1}{}",
            cat(dl),
            dl.get("ResourceArn").and_then(Value::as_str).unwrap_or("")
        );
    }
    format!("other\u{1}{resource}")
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

impl LakeFormationService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate::validate(action, req)?;
        let body = req_body(req);
        match action {
            // LF-tags
            "CreateLFTag" => self.create_lf_tag(req, &body),
            "GetLFTag" => self.get_lf_tag(req, &body),
            "UpdateLFTag" => self.update_lf_tag(req, &body),
            "DeleteLFTag" => self.delete_lf_tag(req, &body),
            "ListLFTags" => self.list_lf_tags(req, &body),
            // LF-tag expressions
            "CreateLFTagExpression" => self.upsert_lf_tag_expression(req, &body, false),
            "UpdateLFTagExpression" => self.upsert_lf_tag_expression(req, &body, true),
            "GetLFTagExpression" => self.get_lf_tag_expression(req, &body),
            "DeleteLFTagExpression" => self.delete_lf_tag_expression(req, &body),
            "ListLFTagExpressions" => self.list_lf_tag_expressions(req, &body),
            // Permissions
            "GrantPermissions" => self.grant_permissions(req, &body),
            "RevokePermissions" => self.revoke_permissions(req, &body),
            "BatchGrantPermissions" => self.batch_permissions(req, &body, true),
            "BatchRevokePermissions" => self.batch_permissions(req, &body, false),
            "ListPermissions" => self.list_permissions(req, &body),
            "GetEffectivePermissionsForPath" => self.effective_permissions(req, &body),
            // Registered resources
            "RegisterResource" => self.register_resource(req, &body),
            "DeregisterResource" => self.deregister_resource(req, &body),
            "DescribeResource" => self.describe_resource(req, &body),
            "UpdateResource" => self.update_resource(req, &body),
            "ListResources" => self.list_resources(req, &body),
            // Data lake settings
            "PutDataLakeSettings" => self.put_data_lake_settings(req, &body),
            "GetDataLakeSettings" => self.get_data_lake_settings(req, &body),
            // Data cells filters
            "CreateDataCellsFilter" => self.create_data_cells_filter(req, &body, false),
            "UpdateDataCellsFilter" => self.create_data_cells_filter(req, &body, true),
            "GetDataCellsFilter" => self.get_data_cells_filter(req, &body),
            "DeleteDataCellsFilter" => self.delete_data_cells_filter(req, &body),
            "ListDataCellsFilter" => self.list_data_cells_filter(req, &body),
            // Opt-ins
            "CreateLakeFormationOptIn" => self.create_opt_in(req, &body),
            "DeleteLakeFormationOptIn" => self.delete_opt_in(req, &body),
            "ListLakeFormationOptIns" => self.list_opt_ins(req, &body),
            // Transactions
            "StartTransaction" => self.start_transaction(req, &body),
            "CommitTransaction" => self.commit_transaction(req, &body),
            "CancelTransaction" => self.cancel_transaction(req, &body),
            "ExtendTransaction" => self.extend_transaction(req, &body),
            "DescribeTransaction" => self.describe_transaction(req, &body),
            "ListTransactions" => self.list_transactions(req, &body),
            // Identity Center
            "CreateLakeFormationIdentityCenterConfiguration" => self.create_idc(req, &body),
            "DescribeLakeFormationIdentityCenterConfiguration" => self.describe_idc(req, &body),
            "UpdateLakeFormationIdentityCenterConfiguration" => self.update_idc(req, &body),
            "DeleteLakeFormationIdentityCenterConfiguration" => self.delete_idc(req, &body),
            // Resource LF-tags
            "AddLFTagsToResource" => self.add_lf_tags_to_resource(req, &body),
            "RemoveLFTagsFromResource" => self.remove_lf_tags_from_resource(req, &body),
            "GetResourceLFTags" => self.get_resource_lf_tags(req, &body),
            // Search
            "SearchDatabasesByLFTags" => self.search_databases(req, &body),
            "SearchTablesByLFTags" => self.search_tables(req, &body),
            // Storage optimizers
            "ListTableStorageOptimizers" => self.list_storage_optimizers(req, &body),
            "UpdateTableStorageOptimizer" => self.update_storage_optimizer(req, &body),
            // Governed-table objects
            "GetTableObjects" => self.get_table_objects(req, &body),
            "UpdateTableObjects" => self.update_table_objects(req, &body),
            "DeleteObjectsOnCancel" => Ok(ok(json!({}))),
            // Credential vending
            "GetTemporaryGlueTableCredentials" => self.temp_glue_table_creds(req, &body),
            "GetTemporaryGluePartitionCredentials" => self.temp_glue_partition_creds(&body),
            "GetTemporaryDataLocationCredentials" => self.temp_data_location_creds(&body),
            "AssumeDecoratedRoleWithSAML" => self.assume_decorated_role(&body),
            "GetDataLakePrincipal" => Ok(ok(json!({
                "Identity": req
                    .principal
                    .as_ref()
                    .map(|p| p.arn.clone())
                    .unwrap_or_else(|| format!("arn:aws:iam::{}:root", req.account_id)),
            }))),
            // Query planning
            "StartQueryPlanning" => self.start_query_planning(req, &body),
            "GetQueryState" => self.get_query_state(req, &body),
            "GetQueryStatistics" => self.get_query_statistics(req, &body),
            "GetWorkUnits" => self.get_work_units(&body),
            // The response's sole member is a `@httpPayload` streaming (Arrow)
            // blob, so the wire body is the raw blob bytes — an empty stream is
            // an empty body, not a JSON object.
            "GetWorkUnitResults" => Ok(ok_empty_blob()),
            _ => Err(AwsServiceError::action_not_implemented(
                "lakeformation",
                action,
            )),
        }
    }

    // ---------------- LF-tags ----------------

    fn create_lf_tag(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let tag_key = str_field(body, "TagKey").unwrap_or_default();
        let tag_values = string_list(body, "TagValues");
        let key = ckey(&[&catalog, &tag_key]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        // The model does not declare `AlreadyExistsException` for CreateLFTag,
        // so a duplicate is reported via the declared `InvalidInputException`
        // rather than silently overwriting the existing tag's values.
        if st.lf_tags.contains_key(&key) {
            return Err(invalid_input(format!("LF-Tag {tag_key} already exists.")));
        }
        st.lf_tags.insert(
            key,
            LFTagRecord {
                catalog_id: catalog,
                tag_key,
                tag_values,
            },
        );
        Ok(ok(json!({})))
    }

    fn get_lf_tag(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let tag_key = str_field(body, "TagKey").unwrap_or_default();
        let key = ckey(&[&catalog, &tag_key]);
        let accounts = self.state.read();
        let tag = accounts
            .get(&req.account_id)
            .and_then(|st| st.lf_tags.get(&key))
            .ok_or_else(|| entity_not_found(format!("Tag {tag_key} does not exist.")))?;
        Ok(ok(json!({
            "CatalogId": tag.catalog_id,
            "TagKey": tag.tag_key,
            "TagValues": tag.tag_values,
        })))
    }

    fn update_lf_tag(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let tag_key = str_field(body, "TagKey").unwrap_or_default();
        let to_add = string_list(body, "TagValuesToAdd");
        let to_delete = string_list(body, "TagValuesToDelete");
        let key = ckey(&[&catalog, &tag_key]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tag = st
            .lf_tags
            .get_mut(&key)
            .ok_or_else(|| entity_not_found(format!("Tag {tag_key} does not exist.")))?;
        tag.tag_values.retain(|v| !to_delete.contains(v));
        for v in to_add {
            if !tag.tag_values.contains(&v) {
                tag.tag_values.push(v);
            }
        }
        Ok(ok(json!({})))
    }

    fn delete_lf_tag(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let tag_key = str_field(body, "TagKey").unwrap_or_default();
        let key = ckey(&[&catalog, &tag_key]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.lf_tags.remove(&key).is_none() {
            return Err(entity_not_found(format!("Tag {tag_key} does not exist.")));
        }
        Ok(ok(json!({})))
    }

    fn list_lf_tags(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.lf_tags
                    .values()
                    .filter(|t| t.catalog_id == catalog)
                    .map(|t| {
                        json!({
                            "CatalogId": t.catalog_id,
                            "TagKey": t.tag_key,
                            "TagValues": t.tag_values,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "LFTags", body, &[])
    }

    // ---------------- LF-tag expressions ----------------

    fn upsert_lf_tag_expression(
        &self,
        req: &AwsRequest,
        body: &Value,
        update: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let name = str_field(body, "Name").unwrap_or_default();
        let key = ckey(&[&catalog, &name]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let exists = st.lf_tag_expressions.contains_key(&key);
        // Create and Update are distinct: creating an existing expression is a
        // conflict, updating a missing one is not-found. `AlreadyExistsException`
        // is not declared for CreateLFTagExpression, so the create conflict uses
        // the declared `InvalidInputException`; the update-missing path uses the
        // declared `EntityNotFoundException`.
        if update && !exists {
            return Err(entity_not_found(format!(
                "LF-Tag expression {name} not found."
            )));
        }
        if !update && exists {
            return Err(invalid_input(format!(
                "LF-Tag expression {name} already exists."
            )));
        }
        let record = LFTagExpressionRecord {
            catalog_id: catalog,
            name,
            description: str_field(body, "Description"),
            expression: body
                .get("Expression")
                .cloned()
                .unwrap_or(Value::Array(vec![])),
        };
        st.lf_tag_expressions.insert(key, record);
        Ok(ok(json!({})))
    }

    fn get_lf_tag_expression(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let name = str_field(body, "Name").unwrap_or_default();
        let key = ckey(&[&catalog, &name]);
        let accounts = self.state.read();
        let e = accounts
            .get(&req.account_id)
            .and_then(|st| st.lf_tag_expressions.get(&key))
            .ok_or_else(|| entity_not_found(format!("LF-Tag expression {name} not found.")))?;
        let mut out = json!({
            "Name": e.name,
            "CatalogId": e.catalog_id,
            "Expression": e.expression,
        });
        if let Some(d) = &e.description {
            out["Description"] = json!(d);
        }
        Ok(ok(out))
    }

    fn delete_lf_tag_expression(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let name = str_field(body, "Name").unwrap_or_default();
        let key = ckey(&[&catalog, &name]);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.lf_tag_expressions.remove(&key).is_none() {
            return Err(entity_not_found(format!(
                "LF-Tag expression {name} not found."
            )));
        }
        Ok(ok(json!({})))
    }

    fn list_lf_tag_expressions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.lf_tag_expressions
                    .values()
                    .filter(|e| e.catalog_id == catalog)
                    .map(|e| {
                        let mut m = json!({
                            "Name": e.name,
                            "CatalogId": e.catalog_id,
                            "Expression": e.expression,
                        });
                        if let Some(d) = &e.description {
                            m["Description"] = json!(d);
                        }
                        m
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "LFTagExpressions", body, &[])
    }

    // ---------------- permissions ----------------

    fn grant_permissions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let principal = body.get("Principal").cloned().unwrap_or(Value::Null);
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        let perms = string_list(body, "Permissions");
        let grant_opt = string_list(body, "PermissionsWithGrantOption");
        let condition = body.get("Condition").cloned();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        apply_grant(
            &mut st.grants,
            catalog,
            principal,
            resource,
            perms,
            grant_opt,
            condition,
            &req.account_id,
        );
        Ok(ok(json!({})))
    }

    fn revoke_permissions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let principal = body.get("Principal").cloned().unwrap_or(Value::Null);
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        let perms = string_list(body, "Permissions");
        let grant_opt = string_list(body, "PermissionsWithGrantOption");
        let condition = body.get("Condition").cloned();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        apply_revoke(
            &mut st.grants,
            &principal,
            &resource,
            &perms,
            &grant_opt,
            &condition,
            &req.account_id,
        );
        Ok(ok(json!({})))
    }

    fn batch_permissions(
        &self,
        req: &AwsRequest,
        body: &Value,
        grant: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let entries = body
            .get("Entries")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        for entry in &entries {
            let principal = entry.get("Principal").cloned().unwrap_or(Value::Null);
            let resource = entry.get("Resource").cloned().unwrap_or(Value::Null);
            let permissions = string_list(entry, "Permissions");
            let grant_opt = string_list(entry, "PermissionsWithGrantOption");
            let condition = entry.get("Condition").cloned();
            if grant {
                apply_grant(
                    &mut st.grants,
                    catalog.clone(),
                    principal,
                    resource,
                    permissions,
                    grant_opt,
                    condition,
                    &req.account_id,
                );
            } else {
                apply_revoke(
                    &mut st.grants,
                    &principal,
                    &resource,
                    &permissions,
                    &grant_opt,
                    &condition,
                    &req.account_id,
                );
            }
        }
        // No per-entry failures for well-formed synthetic input.
        Ok(ok(json!({ "Failures": [] })))
    }

    fn list_permissions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let principal_filter = body.get("Principal").cloned();
        let resource_filter = body.get("Resource").cloned();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.grants
                    .iter()
                    .filter(|g| {
                        principal_filter
                            .as_ref()
                            .is_none_or(|p| principal_matches(p, &g.principal))
                    })
                    .filter(|g| {
                        resource_filter
                            .as_ref()
                            .is_none_or(|r| resource_matches(r, &g.resource, &req.account_id))
                    })
                    .map(grant_to_value)
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "PrincipalResourcePermissions", body, &[])
    }

    fn effective_permissions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_field(body, "ResourceArn").unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.grants
                    .iter()
                    .filter(|g| grant_touches_arn(&g.resource, &arn))
                    .map(grant_to_value)
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "Permissions", body, &[])
    }

    // ---------------- registered resources ----------------

    fn register_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_field(body, "ResourceArn").unwrap_or_default();
        let use_slr = body
            .get("UseServiceLinkedRole")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let role_arn = str_field(body, "RoleArn").or(if use_slr {
            Some(format!(
                "arn:aws:iam::{}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess",
                req.account_id
            ))
        } else {
            None
        });
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.resources.contains_key(&arn) {
            return Err(already_exists(format!(
                "Resource {arn} is already registered."
            )));
        }
        st.resources.insert(
            arn.clone(),
            ResourceInfoRecord {
                resource_arn: arn,
                role_arn,
                last_modified: Utc::now(),
                with_federation: body.get("WithFederation").and_then(Value::as_bool),
                hybrid_access_enabled: body.get("HybridAccessEnabled").and_then(Value::as_bool),
                with_privileged_access: body.get("WithPrivilegedAccess").and_then(Value::as_bool),
                expected_resource_owner_account: str_field(body, "ExpectedResourceOwnerAccount"),
            },
        );
        Ok(ok(json!({})))
    }

    fn deregister_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_field(body, "ResourceArn").unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.resources.remove(&arn).is_none() {
            return Err(entity_not_found(format!(
                "Resource {arn} is not registered."
            )));
        }
        Ok(ok(json!({})))
    }

    fn describe_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_field(body, "ResourceArn").unwrap_or_default();
        let accounts = self.state.read();
        let r = accounts
            .get(&req.account_id)
            .and_then(|st| st.resources.get(&arn))
            .ok_or_else(|| entity_not_found(format!("Resource {arn} is not registered.")))?;
        Ok(ok(json!({ "ResourceInfo": resource_info_value(r) })))
    }

    fn update_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_field(body, "ResourceArn").unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let r = st
            .resources
            .get_mut(&arn)
            .ok_or_else(|| entity_not_found(format!("Resource {arn} is not registered.")))?;
        if let Some(role) = str_field(body, "RoleArn") {
            r.role_arn = Some(role);
        }
        if let Some(f) = body.get("WithFederation").and_then(Value::as_bool) {
            r.with_federation = Some(f);
        }
        if let Some(h) = body.get("HybridAccessEnabled").and_then(Value::as_bool) {
            r.hybrid_access_enabled = Some(h);
        }
        if let Some(p) = body.get("WithPrivilegedAccess").and_then(Value::as_bool) {
            r.with_privileged_access = Some(p);
        }
        if let Some(a) = str_field(body, "ExpectedResourceOwnerAccount") {
            r.expected_resource_owner_account = Some(a);
        }
        r.last_modified = Utc::now();
        Ok(ok(json!({})))
    }

    fn list_resources(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| st.resources.values().map(resource_info_value).collect())
            .unwrap_or_default();
        page_response(items, "ResourceInfoList", body, &[])
    }

    // ---------------- data lake settings ----------------

    fn put_data_lake_settings(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let settings = body.get("DataLakeSettings").cloned().unwrap_or(json!({}));
        self.state
            .write()
            .get_or_create(&req.account_id)
            .data_lake_settings
            .insert(catalog, settings);
        Ok(ok(json!({})))
    }

    fn get_data_lake_settings(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let accounts = self.state.read();
        let settings = accounts
            .get(&req.account_id)
            .and_then(|st| st.data_lake_settings.get(&catalog))
            .cloned()
            .unwrap_or_else(default_data_lake_settings);
        Ok(ok(json!({ "DataLakeSettings": settings })))
    }

    // ---------------- data cells filters ----------------

    fn create_data_cells_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
        update: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let td = body.get("TableData").cloned().unwrap_or(json!({}));
        // `DatabaseName`, `TableName`, and `Name` are `@required` inside the
        // nested `DataCellsFilter` shape. Without them the filter would be keyed
        // off empty parts and become unreachable by Get/Delete (which require
        // non-empty identifiers), so reject them up front.
        let nonempty = |k: &str| {
            td.get(k)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty())
        };
        if !nonempty("DatabaseName") || !nonempty("TableName") || !nonempty("Name") {
            return Err(invalid_input(
                "TableData.DatabaseName, TableData.TableName, and TableData.Name are required and must be non-empty.",
            ));
        }
        let key = data_cells_key_parts(&td, &req.account_id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !update && st.data_cells_filters.contains_key(&key) {
            return Err(already_exists("Data cells filter already exists."));
        }
        if update && !st.data_cells_filters.contains_key(&key) {
            return Err(entity_not_found("Data cells filter does not exist."));
        }
        st.data_cells_filters.insert(key, td);
        Ok(ok(json!({})))
    }

    fn get_data_cells_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = data_cells_key_parts(body, &req.account_id);
        let accounts = self.state.read();
        let f = accounts
            .get(&req.account_id)
            .and_then(|st| st.data_cells_filters.get(&key))
            .cloned()
            .ok_or_else(|| entity_not_found("Data cells filter does not exist."))?;
        Ok(ok(json!({ "DataCellsFilter": f })))
    }

    fn delete_data_cells_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = data_cells_key_parts(body, &req.account_id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.data_cells_filters.remove(&key).is_none() {
            return Err(entity_not_found("Data cells filter does not exist."));
        }
        Ok(ok(json!({})))
    }

    fn list_data_cells_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let table = body.get("Table");
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.data_cells_filters
                    .values()
                    .filter(|f| match table {
                        None => true,
                        Some(t) => {
                            f.get("DatabaseName") == t.get("DatabaseName")
                                && f.get("TableName") == t.get("Name")
                        }
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "DataCellsFilters", body, &[])
    }

    // ---------------- opt-ins ----------------

    fn create_opt_in(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let record = OptInRecord {
            principal: body.get("Principal").cloned().unwrap_or(Value::Null),
            resource: body.get("Resource").cloned().unwrap_or(Value::Null),
            condition: body.get("Condition").cloned(),
            last_modified: Utc::now(),
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .opt_ins
            .push(record);
        Ok(ok(json!({})))
    }

    fn delete_opt_in(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let principal = body.get("Principal").cloned().unwrap_or(Value::Null);
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        st.opt_ins.retain(|o| {
            !(principal_matches(&principal, &o.principal)
                && resource_matches(&resource, &o.resource, &req.account_id))
        });
        Ok(ok(json!({})))
    }

    fn list_opt_ins(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let principal_filter = body.get("Principal").cloned();
        let resource_filter = body.get("Resource").cloned();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.opt_ins
                    .iter()
                    .filter(|o| {
                        principal_filter
                            .as_ref()
                            .is_none_or(|p| principal_matches(p, &o.principal))
                    })
                    .filter(|o| {
                        resource_filter
                            .as_ref()
                            .is_none_or(|r| resource_matches(r, &o.resource, &req.account_id))
                    })
                    .map(|o| {
                        let mut m = json!({
                            "Resource": o.resource,
                            "Principal": o.principal,
                            "LastModified": ts(o.last_modified),
                            "LastUpdatedBy": format!("arn:aws:iam::{}:root", req.account_id),
                        });
                        if let Some(c) = &o.condition {
                            m["Condition"] = c.clone();
                        }
                        m
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "LakeFormationOptInsInfoList", body, &[])
    }

    // ---------------- transactions ----------------

    fn start_transaction(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tx_id = format!("tx:{}", gen_uuid().replace('-', ""));
        let record = TransactionRecord {
            transaction_id: tx_id.clone(),
            transaction_type: str_field(body, "TransactionType")
                .unwrap_or_else(|| "READ_AND_WRITE".to_string()),
            status: "ACTIVE".to_string(),
            start_time: Utc::now(),
            end_time: None,
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .transactions
            .insert(tx_id.clone(), record);
        Ok(ok(json!({ "TransactionId": tx_id })))
    }

    fn commit_transaction(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tx_id = str_field(body, "TransactionId").unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tx = st
            .transactions
            .get_mut(&tx_id)
            .ok_or_else(|| entity_not_found(format!("Transaction {tx_id} not found.")))?;
        match tx.status.as_str() {
            // Committing an aborted transaction is an error (declared
            // `TransactionCanceledException`); committing an already-committed
            // one is idempotent and returns its terminal status.
            "ABORTED" => {
                return Err(err(
                    StatusCode::BAD_REQUEST,
                    "TransactionCanceledException",
                    format!("Transaction {tx_id} was previously canceled."),
                ))
            }
            "ACTIVE" => {
                tx.status = "COMMITTED".to_string();
                tx.end_time = Some(Utc::now());
            }
            _ => {}
        }
        Ok(ok(json!({ "TransactionStatus": tx.status })))
    }

    fn cancel_transaction(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tx_id = str_field(body, "TransactionId").unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let tx = st
            .transactions
            .get_mut(&tx_id)
            .ok_or_else(|| entity_not_found(format!("Transaction {tx_id} not found.")))?;
        if tx.status == "COMMITTED" {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "TransactionCommittedException",
                "Transaction already committed.",
            ));
        }
        tx.status = "ABORTED".to_string();
        tx.end_time = Some(Utc::now());
        Ok(ok(json!({})))
    }

    fn extend_transaction(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        if let Some(tx_id) = str_field(body, "TransactionId") {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let tx = st
                .transactions
                .get_mut(&tx_id)
                .ok_or_else(|| entity_not_found(format!("Transaction {tx_id} not found.")))?;
            // Only an ACTIVE transaction can be extended; a terminal one errors
            // with the declared code for its state.
            match tx.status.as_str() {
                "ABORTED" => {
                    return Err(err(
                        StatusCode::BAD_REQUEST,
                        "TransactionCanceledException",
                        format!("Transaction {tx_id} was previously canceled."),
                    ))
                }
                "COMMITTED" => {
                    return Err(err(
                        StatusCode::BAD_REQUEST,
                        "TransactionCommittedException",
                        format!("Transaction {tx_id} was previously committed."),
                    ))
                }
                _ => {}
            }
        }
        Ok(ok(json!({})))
    }

    fn describe_transaction(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tx_id = str_field(body, "TransactionId").unwrap_or_default();
        let accounts = self.state.read();
        let tx = accounts
            .get(&req.account_id)
            .and_then(|st| st.transactions.get(&tx_id))
            .ok_or_else(|| entity_not_found(format!("Transaction {tx_id} not found.")))?;
        Ok(ok(
            json!({ "TransactionDescription": transaction_value(tx) }),
        ))
    }

    fn list_transactions(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let status_filter = str_field(body, "StatusFilter").unwrap_or_else(|| "ALL".to_string());
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.transactions
                    .values()
                    .filter(|tx| match status_filter.as_str() {
                        "ALL" => true,
                        "COMPLETED" => tx.status == "COMMITTED" || tx.status == "ABORTED",
                        s => tx.status == s,
                    })
                    .map(transaction_value)
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "Transactions", body, &[])
    }

    // ---------------- Identity Center ----------------

    fn create_idc(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let application_arn = format!(
            "arn:aws:sso::{}:application/ssoins-{}/apl-{}",
            req.account_id,
            alphanum(16),
            alphanum(16)
        );
        let record = IdentityCenterRecord {
            catalog_id: catalog.clone(),
            instance_arn: str_field(body, "InstanceArn"),
            application_arn: application_arn.clone(),
            application_status: "ENABLED".to_string(),
            external_filtering: body.get("ExternalFiltering").cloned(),
            share_recipients: body.get("ShareRecipients").cloned(),
            service_integrations: body.get("ServiceIntegrations").cloned(),
            resource_share: None,
        };
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.identity_center.contains_key(&catalog) {
            return Err(already_exists(
                "An IdentityCenter configuration already exists for this catalog.",
            ));
        }
        st.identity_center.insert(catalog, record);
        Ok(ok(json!({ "ApplicationArn": application_arn })))
    }

    fn describe_idc(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let accounts = self.state.read();
        let c = accounts
            .get(&req.account_id)
            .and_then(|st| st.identity_center.get(&catalog))
            .ok_or_else(|| entity_not_found("No IdentityCenter configuration found."))?;
        let mut out = json!({
            "CatalogId": c.catalog_id,
            "ApplicationArn": c.application_arn,
        });
        if let Some(v) = &c.instance_arn {
            out["InstanceArn"] = json!(v);
        }
        if let Some(v) = &c.external_filtering {
            out["ExternalFiltering"] = v.clone();
        }
        if let Some(v) = &c.share_recipients {
            out["ShareRecipients"] = v.clone();
        }
        if let Some(v) = &c.service_integrations {
            out["ServiceIntegrations"] = v.clone();
        }
        if let Some(v) = &c.resource_share {
            out["ResourceShare"] = json!(v);
        }
        Ok(ok(out))
    }

    fn update_idc(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let c = st
            .identity_center
            .get_mut(&catalog)
            .ok_or_else(|| entity_not_found("No IdentityCenter configuration found."))?;
        if let Some(v) = body.get("ShareRecipients") {
            c.share_recipients = Some(v.clone());
        }
        if let Some(v) = body.get("ServiceIntegrations") {
            c.service_integrations = Some(v.clone());
        }
        if let Some(v) = body.get("ExternalFiltering") {
            c.external_filtering = Some(v.clone());
        }
        if let Some(v) = str_field(body, "ApplicationStatus") {
            c.application_status = v;
        }
        Ok(ok(json!({})))
    }

    fn delete_idc(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if st.identity_center.remove(&catalog).is_none() {
            return Err(entity_not_found("No IdentityCenter configuration found."));
        }
        Ok(ok(json!({})))
    }

    // ---------------- resource LF-tags ----------------

    fn add_lf_tags_to_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        let sig = resource_signature(&resource, &req.account_id);
        let catalog = catalog_id(body, &req.account_id);
        let new_tags: Vec<Value> = body
            .get("LFTags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|t| tag_pair_value(&t, &catalog))
            .collect();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let entry = st.resource_lf_tags.entry(sig).or_default();
        for t in new_tags {
            let key = t.get("TagKey").cloned();
            entry.retain(|e| e.get("TagKey") != key.as_ref());
            entry.push(t);
        }
        Ok(ok(json!({ "Failures": [] })))
    }

    fn remove_lf_tags_from_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        let sig = resource_signature(&resource, &req.account_id);
        let remove_keys: Vec<Value> = body
            .get("LFTags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|t| t.get("TagKey").cloned())
            .collect();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if let Some(entry) = st.resource_lf_tags.get_mut(&sig) {
            entry.retain(|e| !remove_keys.iter().any(|k| e.get("TagKey") == Some(k)));
        }
        Ok(ok(json!({ "Failures": [] })))
    }

    fn get_resource_lf_tags(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource = body.get("Resource").cloned().unwrap_or(Value::Null);
        let sig = resource_signature(&resource, &req.account_id);
        let accounts = self.state.read();
        let tags: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|st| st.resource_lf_tags.get(&sig))
            .cloned()
            .unwrap_or_default();
        let mut out = Map::new();
        if resource.get("Database").is_some() {
            out.insert("LFTagOnDatabase".to_string(), Value::Array(tags));
        } else if resource.get("Table").is_some() {
            out.insert("LFTagsOnTable".to_string(), Value::Array(tags));
        } else if let Some(twc) = resource.get("TableWithColumns") {
            // Fan the stored tags out to one `ColumnLFTag` per column name in
            // the request, rather than collapsing them under the first column.
            let col_tags: Vec<Value> = twc
                .get("ColumnNames")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>())
                .unwrap_or_default()
                .into_iter()
                .map(|col| json!({ "Name": col, "LFTags": tags.clone() }))
                .collect();
            out.insert("LFTagsOnColumns".to_string(), Value::Array(col_tags));
        } else {
            out.insert("LFTagsOnTable".to_string(), Value::Array(tags));
        }
        Ok(ok(Value::Object(out)))
    }

    // ---------------- search ----------------

    fn search_databases(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        // `Expression` is `@required`; an omitted or empty condition list is
        // rejected rather than treated as a match-everything wildcard.
        let expression = body
            .get("Expression")
            .cloned()
            .unwrap_or(Value::Array(vec![]));
        if expression.as_array().is_none_or(|a| a.is_empty()) {
            return Err(invalid_input("Expression cannot be empty."));
        }
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.resource_lf_tags
                    .iter()
                    .filter_map(|(sig, tags)| {
                        let parts: Vec<&str> = sig.split('\u{1}').collect();
                        // `db\u{1}{catalog}\u{1}{name}` — Database resources only.
                        if parts.first() != Some(&"db") {
                            return None;
                        }
                        let cat = parts.get(1).copied().unwrap_or("");
                        let name = parts.get(2).copied().unwrap_or("");
                        if cat != catalog || !expression_satisfied(tags, &expression) {
                            return None;
                        }
                        Some(json!({
                            "Database": { "CatalogId": cat, "Name": name },
                            "LFTags": tags,
                        }))
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "DatabaseList", body, &[])
    }

    fn search_tables(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let catalog = catalog_id(body, &req.account_id);
        // `Expression` is `@required`; an omitted or empty condition list is
        // rejected rather than treated as a match-everything wildcard.
        let expression = body
            .get("Expression")
            .cloned()
            .unwrap_or(Value::Array(vec![]));
        if expression.as_array().is_none_or(|a| a.is_empty()) {
            return Err(invalid_input("Expression cannot be empty."));
        }
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|st| {
                st.resource_lf_tags
                    .iter()
                    .filter_map(|(sig, tags)| {
                        let parts: Vec<&str> = sig.split('\u{1}').collect();
                        // `table\u{1}{catalog}\u{1}{db}\u{1}{name}\u{1}{wildcard}` —
                        // Table resources only.
                        if parts.first() != Some(&"table") {
                            return None;
                        }
                        let cat = parts.get(1).copied().unwrap_or("");
                        let db = parts.get(2).copied().unwrap_or("");
                        let name = parts.get(3).copied().unwrap_or("");
                        if cat != catalog || !expression_satisfied(tags, &expression) {
                            return None;
                        }
                        Some(json!({
                            "Table": { "CatalogId": cat, "DatabaseName": db, "Name": name },
                            "LFTagsOnTable": tags,
                        }))
                    })
                    .collect()
            })
            .unwrap_or_default();
        page_response(items, "TableList", body, &[])
    }

    // ---------------- storage optimizers ----------------

    fn list_storage_optimizers(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = ckey(&[
            &catalog_id(body, &req.account_id),
            &str_field(body, "DatabaseName").unwrap_or_default(),
            &str_field(body, "TableName").unwrap_or_default(),
        ]);
        let accounts = self.state.read();
        let stored = accounts
            .get(&req.account_id)
            .and_then(|st| st.storage_optimizers.get(&key))
            .cloned();
        // Report the standard optimizer families; overlay any stored config.
        let mut items = Vec::new();
        for family in ["compaction", "garbage_collection"] {
            let config = stored
                .as_ref()
                .and_then(|m| m.get(family))
                .cloned()
                .unwrap_or_else(|| json!({ "is_enabled": "false" }));
            items.push(json!({
                "StorageOptimizerType": family.to_uppercase(),
                "Config": config,
            }));
        }
        page_response(items, "StorageOptimizerList", body, &[])
    }

    fn update_storage_optimizer(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = ckey(&[
            &catalog_id(body, &req.account_id),
            &str_field(body, "DatabaseName").unwrap_or_default(),
            &str_field(body, "TableName").unwrap_or_default(),
        ]);
        let config = body
            .get("StorageOptimizerConfig")
            .cloned()
            .unwrap_or(json!({}));
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let entry = st
            .storage_optimizers
            .entry(key)
            .or_insert_with(|| json!({}));
        if let (Some(obj), Some(cfg)) = (entry.as_object_mut(), config.as_object()) {
            for (k, v) in cfg {
                obj.insert(k.to_lowercase(), v.clone());
            }
        }
        Ok(ok(
            json!({ "Result": "Successfully updated the storage optimizer." }),
        ))
    }

    // ---------------- governed-table objects ----------------

    fn get_table_objects(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = ckey(&[
            &catalog_id(body, &req.account_id),
            &str_field(body, "DatabaseName").unwrap_or_default(),
            &str_field(body, "TableName").unwrap_or_default(),
        ]);
        let accounts = self.state.read();
        let objects: Vec<Value> = accounts
            .get(&req.account_id)
            .and_then(|st| st.table_objects.get(&key))
            .cloned()
            .unwrap_or_default();
        let partitioned = if objects.is_empty() {
            Vec::new()
        } else {
            vec![json!({ "PartitionValues": [], "Objects": objects })]
        };
        page_response(partitioned, "Objects", body, &[])
    }

    fn update_table_objects(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = ckey(&[
            &catalog_id(body, &req.account_id),
            &str_field(body, "DatabaseName").unwrap_or_default(),
            &str_field(body, "TableName").unwrap_or_default(),
        ]);
        let write_ops = body
            .get("WriteOperations")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let objects = st.table_objects.entry(key).or_default();
        for w in &write_ops {
            if let Some(add) = w.get("AddObject") {
                objects.push(add.clone());
            }
            if let Some(del) = w.get("DeleteObject") {
                // A `DeleteObjectInput` identifies a specific object version by
                // both its URI and ETag, so only the exact (Uri, ETag) match is
                // removed — other versions of the same URI are preserved.
                let uri = del.get("Uri");
                let etag = del.get("ETag");
                objects.retain(|o| !(o.get("Uri") == uri && o.get("ETag") == etag));
            }
        }
        Ok(ok(json!({})))
    }

    // ---------------- credential vending ----------------

    fn temp_glue_table_creds(
        &self,
        _req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let c = vend_credentials(body.get("DurationSeconds").and_then(Value::as_i64));
        let mut out = creds_value(&c);
        if let Some(s3) = str_field(body, "S3Path") {
            out["VendedS3Path"] = json!([s3]);
        }
        Ok(ok(out))
    }

    fn temp_glue_partition_creds(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let c = vend_credentials(body.get("DurationSeconds").and_then(Value::as_i64));
        Ok(ok(creds_value(&c)))
    }

    fn temp_data_location_creds(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let c = vend_credentials(body.get("DurationSeconds").and_then(Value::as_i64));
        let mut out = json!({
            "Credentials": {
                "AccessKeyId": c.access_key_id,
                "SecretAccessKey": c.secret_access_key,
                "SessionToken": c.session_token,
                "Expiration": ts(c.expiration),
            },
        });
        if let Some(scope) = str_field(body, "CredentialsScope") {
            out["CredentialsScope"] = json!(scope);
        }
        Ok(ok(out))
    }

    fn assume_decorated_role(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let c = vend_credentials(body.get("DurationSeconds").and_then(Value::as_i64));
        Ok(ok(creds_value(&c)))
    }

    // ---------------- query planning ----------------

    fn start_query_planning(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let query_id = gen_uuid();
        self.state
            .write()
            .get_or_create(&req.account_id)
            .queries
            .insert(
                query_id.clone(),
                QueryRecord {
                    query_id: query_id.clone(),
                    state: "WORKUNITS_AVAILABLE".to_string(),
                    submission_time: Utc::now(),
                },
            );
        Ok(ok(json!({ "QueryId": query_id })))
    }

    fn get_query_state(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let query_id = str_field(body, "QueryId").unwrap_or_default();
        let accounts = self.state.read();
        // A QueryId that was never issued is not a finished query. GetQueryState
        // does not declare `EntityNotFoundException`, so the unknown-query case
        // surfaces via the declared `InvalidInputException`.
        let state = accounts
            .get(&req.account_id)
            .and_then(|st| st.queries.get(&query_id))
            .map(|q| q.state.clone())
            .ok_or_else(|| invalid_input(format!("Query {query_id} not found.")))?;
        Ok(ok(json!({ "State": state })))
    }

    fn get_query_statistics(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let query_id = str_field(body, "QueryId").unwrap_or_default();
        let submitted = self
            .state
            .read()
            .get(&req.account_id)
            .and_then(|st| st.queries.get(&query_id))
            .map(|q| q.submission_time)
            .unwrap_or_else(Utc::now);
        Ok(ok(json!({
            "ExecutionStatistics": {
                "AverageExecutionTimeMillis": 0,
                "DataScannedBytes": 0,
                "WorkUnitsExecutedCount": 0,
            },
            "PlanningStatistics": {
                "EstimatedDataToScanBytes": 0,
                "PlanningTimeMillis": 0,
                "QueueTimeMillis": 0,
                "WorkUnitsGeneratedCount": 0,
            },
            "QuerySubmissionTime": ts(submitted),
        })))
    }

    fn get_work_units(&self, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let query_id = str_field(body, "QueryId").unwrap_or_default();
        Ok(ok(json!({
            "QueryId": query_id,
            "WorkUnitRanges": [{
                "WorkUnitIdMin": 0,
                "WorkUnitIdMax": 0,
                "WorkUnitToken": alphanum(32),
            }],
        })))
    }
}

// ---------------------------------------------------------------------------
// Value builders + matchers
// ---------------------------------------------------------------------------

fn creds_value(c: &TempCreds) -> Value {
    json!({
        "AccessKeyId": c.access_key_id,
        "SecretAccessKey": c.secret_access_key,
        "SessionToken": c.session_token,
        "Expiration": ts(c.expiration),
    })
}

fn resource_info_value(r: &ResourceInfoRecord) -> Value {
    let mut m = json!({
        "ResourceArn": r.resource_arn,
        "LastModified": ts(r.last_modified),
    });
    if let Some(role) = &r.role_arn {
        m["RoleArn"] = json!(role);
    }
    if let Some(f) = r.with_federation {
        m["WithFederation"] = json!(f);
    }
    if let Some(h) = r.hybrid_access_enabled {
        m["HybridAccessEnabled"] = json!(h);
    }
    if let Some(p) = r.with_privileged_access {
        m["WithPrivilegedAccess"] = json!(p);
    }
    if let Some(a) = &r.expected_resource_owner_account {
        m["ExpectedResourceOwnerAccount"] = json!(a);
    }
    m
}

fn transaction_value(tx: &TransactionRecord) -> Value {
    let mut m = json!({
        "TransactionId": tx.transaction_id,
        "TransactionStatus": tx.status,
        "TransactionStartTime": ts(tx.start_time),
    });
    if let Some(end) = tx.end_time {
        m["TransactionEndTime"] = ts(end);
    }
    m
}

fn grant_to_value(g: &GrantRecord) -> Value {
    let mut m = json!({
        "Principal": g.principal,
        "Resource": g.resource,
        "Permissions": g.permissions,
        "PermissionsWithGrantOption": g.permissions_with_grant_option,
        "LastUpdated": ts(g.last_updated),
    });
    if let Some(c) = &g.condition {
        m["Condition"] = c.clone();
    }
    m
}

fn tag_pair_value(t: &Value, catalog: &str) -> Value {
    json!({
        "CatalogId": t.get("CatalogId").and_then(Value::as_str).unwrap_or(catalog),
        "TagKey": t.get("TagKey").cloned().unwrap_or(Value::Null),
        "TagValues": t.get("TagValues").cloned().unwrap_or(Value::Array(vec![])),
    })
}

/// Build a data-cell-filter storage key from an object carrying
/// `TableCatalogId` / `DatabaseName` / `TableName` / `Name`. `TableCatalogId`
/// defaults to `account_id` consistently across create/update (which read the
/// nested `TableData`) and get/delete (which read the top-level request), so a
/// filter created without an explicit catalog id stays reachable.
fn data_cells_key_parts(obj: &Value, account_id: &str) -> String {
    ckey(&[
        obj.get("TableCatalogId")
            .and_then(Value::as_str)
            .unwrap_or(account_id),
        obj.get("DatabaseName")
            .and_then(Value::as_str)
            .unwrap_or(""),
        obj.get("TableName").and_then(Value::as_str).unwrap_or(""),
        obj.get("Name").and_then(Value::as_str).unwrap_or(""),
    ])
}

fn default_data_lake_settings() -> Value {
    json!({
        "DataLakeAdmins": [],
        "ReadOnlyAdmins": [],
        "CreateDatabaseDefaultPermissions": [{
            "Principal": { "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS" },
            "Permissions": ["ALL"],
        }],
        "CreateTableDefaultPermissions": [{
            "Principal": { "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS" },
            "Permissions": ["ALL"],
        }],
        "Parameters": {},
        "TrustedResourceOwners": [],
    })
}

/// Whether a principal filter matches a stored grant/opt-in principal by their
/// `DataLakePrincipalIdentifier`.
fn principal_matches(filter: &Value, stored: &Value) -> bool {
    let id = |v: &Value| {
        v.get("DataLakePrincipalIdentifier")
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    match (id(filter), id(stored)) {
        (Some(a), Some(b)) => a == b,
        _ => filter == stored,
    }
}

/// Whether a `Resource` filter matches a stored resource by their full logical
/// identity (catalog id + names + column set), via the resource signature.
fn resource_matches(filter: &Value, stored: &Value, account_id: &str) -> bool {
    resource_signature(filter, account_id) == resource_signature(stored, account_id)
}

/// Whether a resource's attached LF-tags satisfy a search `Expression`. The
/// expression is a list of `{TagKey, TagValues}` terms; the resource matches
/// when, for every term, it carries that `TagKey` with at least one value in
/// the term's `TagValues` (AND across terms, matching AWS LF-tag expression
/// semantics). An empty expression matches every tagged resource.
fn expression_satisfied(tags: &[Value], expression: &Value) -> bool {
    let Some(terms) = expression.as_array() else {
        return false;
    };
    terms.iter().all(|term| {
        let key = term.get("TagKey").and_then(Value::as_str);
        let wanted: Vec<&str> = term
            .get("TagValues")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        tags.iter().any(|t| {
            t.get("TagKey").and_then(Value::as_str) == key
                && t.get("TagValues")
                    .and_then(Value::as_array)
                    .is_some_and(|vals| {
                        vals.iter()
                            .filter_map(Value::as_str)
                            .any(|v| wanted.contains(&v))
                    })
        })
    })
}

/// Whether a stored grant's resource references the given ARN (used by
/// `GetEffectivePermissionsForPath`, which filters by a data-location ARN).
fn grant_touches_arn(resource: &Value, arn: &str) -> bool {
    if let Some(dl) = resource.get("DataLocation") {
        return dl.get("ResourceArn").and_then(Value::as_str) == Some(arn);
    }
    false
}

/// Apply a grant, merging into an existing grant for the same
/// (principal, resource, condition) rather than creating a duplicate row: the
/// requested permissions and grant-option permissions are unioned into it.
/// Lake Formation grants are set-valued and re-granting is idempotent, so
/// `ListPermissions` never surfaces two rows for one logical grant.
#[allow(clippy::too_many_arguments)]
fn apply_grant(
    grants: &mut Vec<GrantRecord>,
    catalog: String,
    principal: Value,
    resource: Value,
    perms: Vec<String>,
    grant_opt: Vec<String>,
    condition: Option<Value>,
    account_id: &str,
) {
    if let Some(g) = grants.iter_mut().find(|g| {
        principal_matches(&principal, &g.principal)
            && resource_matches(&resource, &g.resource, account_id)
            && g.condition == condition
    }) {
        for p in perms {
            if !g.permissions.contains(&p) {
                g.permissions.push(p);
            }
        }
        for p in grant_opt {
            if !g.permissions_with_grant_option.contains(&p) {
                g.permissions_with_grant_option.push(p);
            }
        }
        g.last_updated = Utc::now();
    } else {
        grants.push(GrantRecord {
            catalog_id: catalog,
            principal,
            resource,
            permissions: perms,
            permissions_with_grant_option: grant_opt,
            condition,
            last_updated: Utc::now(),
        });
    }
}

/// Revoke permissions with set semantics: from every grant matching the
/// principal + resource + condition, remove the named permissions and
/// grant-option permissions (order-independent). The condition is matched
/// symmetrically with `apply_grant`'s keying, so a revoke targeting one
/// condition-scoped grant never sweeps a sibling grant that differs only by
/// `Condition`. Grants left with no permissions at all are dropped; grants that
/// retain some permissions are kept with the remainder.
fn apply_revoke(
    grants: &mut Vec<GrantRecord>,
    principal: &Value,
    resource: &Value,
    perms: &[String],
    grant_opt: &[String],
    condition: &Option<Value>,
    account_id: &str,
) {
    for g in grants.iter_mut() {
        if principal_matches(principal, &g.principal)
            && resource_matches(resource, &g.resource, account_id)
            && &g.condition == condition
        {
            g.permissions.retain(|p| !perms.contains(p));
            g.permissions_with_grant_option
                .retain(|p| !grant_opt.contains(p));
        }
    }
    grants.retain(|g| !(g.permissions.is_empty() && g.permissions_with_grant_option.is_empty()));
}

/// A 200 response with an empty body, for operations whose sole output member
/// is a `@httpPayload` streaming blob (an empty stream is an empty body).
fn ok_empty_blob() -> AwsResponse {
    AwsResponse::json(StatusCode::OK, Vec::<u8>::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;

    const ACCOUNT: &str = "123456789012";

    fn svc() -> LakeFormationService {
        LakeFormationService::new(Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "",
        ))))
    }

    fn req(body: Value) -> AwsRequest {
        AwsRequest {
            service: "lakeformation".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: ACCOUNT.to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match r {
            Ok(_) => panic!("expected an error, got Ok"),
            Err(e) => e,
        }
    }

    #[test]
    fn update_resource_persists_expected_owner_and_privileged_access() {
        let svc = svc();
        let arn = "arn:aws:s3:::my-data-lake";
        let reg = json!({
            "ResourceArn": arn,
            "RoleArn": "arn:aws:iam::123456789012:role/lf-role",
        });
        svc.register_resource(&req(reg.clone()), &reg).unwrap();

        let upd = json!({
            "ResourceArn": arn,
            "RoleArn": "arn:aws:iam::123456789012:role/lf-role",
            "ExpectedResourceOwnerAccount": "210987654321",
            "WithPrivilegedAccess": true,
        });
        svc.update_resource(&req(upd.clone()), &upd).unwrap();

        let desc = json!({ "ResourceArn": arn });
        let out = body_of(svc.describe_resource(&req(desc.clone()), &desc).unwrap());
        assert_eq!(
            out["ResourceInfo"]["ExpectedResourceOwnerAccount"],
            "210987654321"
        );
        assert_eq!(out["ResourceInfo"]["WithPrivilegedAccess"], true);
    }

    #[test]
    fn search_databases_finds_matching_tagged_resource() {
        let svc = svc();
        let tag_body = json!({
            "Resource": { "Database": { "Name": "sales" } },
            "LFTags": [{ "TagKey": "env", "TagValues": ["prod"] }],
        });
        svc.add_lf_tags_to_resource(&req(tag_body.clone()), &tag_body)
            .unwrap();

        // Matching expression finds the tagged database.
        let hit = json!({ "Expression": [{ "TagKey": "env", "TagValues": ["prod"] }] });
        let out = body_of(svc.search_databases(&req(hit.clone()), &hit).unwrap());
        let list = out["DatabaseList"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["Database"]["Name"], "sales");
        assert_eq!(list[0]["LFTags"][0]["TagKey"], "env");

        // Non-matching value returns nothing.
        let miss = json!({ "Expression": [{ "TagKey": "env", "TagValues": ["dev"] }] });
        let out = body_of(svc.search_databases(&req(miss.clone()), &miss).unwrap());
        assert!(out["DatabaseList"].as_array().unwrap().is_empty());

        // Non-matching key returns nothing.
        let miss_key = json!({ "Expression": [{ "TagKey": "team", "TagValues": ["prod"] }] });
        let out = body_of(
            svc.search_databases(&req(miss_key.clone()), &miss_key)
                .unwrap(),
        );
        assert!(out["DatabaseList"].as_array().unwrap().is_empty());
    }

    #[test]
    fn search_tables_finds_matching_tagged_table() {
        let svc = svc();
        let tag_body = json!({
            "Resource": { "Table": { "DatabaseName": "db", "Name": "orders" } },
            "LFTags": [{ "TagKey": "tier", "TagValues": ["gold", "silver"] }],
        });
        svc.add_lf_tags_to_resource(&req(tag_body.clone()), &tag_body)
            .unwrap();

        let hit = json!({ "Expression": [{ "TagKey": "tier", "TagValues": ["gold"] }] });
        let out = body_of(svc.search_tables(&req(hit.clone()), &hit).unwrap());
        let list = out["TableList"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["Table"]["Name"], "orders");
        assert_eq!(list[0]["Table"]["DatabaseName"], "db");

        // A Database search must not return the tagged Table.
        let out = body_of(svc.search_databases(&req(hit.clone()), &hit).unwrap());
        assert!(out["DatabaseList"].as_array().unwrap().is_empty());
    }

    #[test]
    fn search_rejects_missing_or_empty_expression() {
        let svc = svc();
        // A tagged resource exists, so a match-all bug would leak it.
        let tag_body = json!({
            "Resource": { "Database": { "Name": "sales" } },
            "LFTags": [{ "TagKey": "env", "TagValues": ["prod"] }],
        });
        svc.add_lf_tags_to_resource(&req(tag_body.clone()), &tag_body)
            .unwrap();

        // Missing Expression.
        let none = json!({});
        assert_eq!(
            err_of(svc.search_databases(&req(none.clone()), &none)).code(),
            "InvalidInputException"
        );
        assert_eq!(
            err_of(svc.search_tables(&req(none.clone()), &none)).code(),
            "InvalidInputException"
        );

        // Empty Expression list.
        let empty = json!({ "Expression": [] });
        assert_eq!(
            err_of(svc.search_databases(&req(empty.clone()), &empty)).code(),
            "InvalidInputException"
        );
        assert_eq!(
            err_of(svc.search_tables(&req(empty.clone()), &empty)).code(),
            "InvalidInputException"
        );
    }

    #[test]
    fn create_data_cells_filter_rejects_empty_table_data() {
        let svc = svc();
        let empty = json!({ "TableData": {} });
        let e = err_of(svc.create_data_cells_filter(&req(empty.clone()), &empty, false));
        assert_eq!(e.code(), "InvalidInputException");

        // Missing just `Name` is also rejected.
        let partial =
            json!({ "TableData": { "DatabaseName": "db", "TableName": "t", "Name": "" } });
        let e = err_of(svc.create_data_cells_filter(&req(partial.clone()), &partial, false));
        assert_eq!(e.code(), "InvalidInputException");

        // A well-formed filter is accepted and round-trips.
        let ok_body = json!({
            "TableData": { "DatabaseName": "db", "TableName": "t", "Name": "f1" }
        });
        svc.create_data_cells_filter(&req(ok_body.clone()), &ok_body, false)
            .unwrap();
        let get = json!({ "TableCatalogId": ACCOUNT, "DatabaseName": "db", "TableName": "t", "Name": "f1" });
        let out = body_of(svc.get_data_cells_filter(&req(get.clone()), &get).unwrap());
        assert_eq!(out["DataCellsFilter"]["Name"], "f1");
    }

    #[test]
    fn get_query_state_unknown_id_errors() {
        let svc = svc();
        let b = json!({ "QueryId": "00000000-0000-0000-0000-000000000000" });
        let e = err_of(svc.get_query_state(&req(b.clone()), &b));
        assert_eq!(e.code(), "InvalidInputException");
    }
}
