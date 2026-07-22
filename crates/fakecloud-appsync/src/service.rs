//! AWS AppSync (`appsync`) restJson1 dispatch + operation handlers.
//!
//! Every AppSync operation is a RESTful `<METHOD> /v1|/v2/...` route with path
//! labels, so requests route on HTTP method + `@http` URI template (segment
//! matching with captured labels). State is account-partitioned and persisted.
//! Sub-resource operations validate their parent GraphQL API and return the
//! declared `NotFoundException` when the parent -- or the addressed resource --
//! is absent, so synthetic conformance ids land on a declared error and real
//! round-trips against a created API succeed.

use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine as _;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use rand::Rng;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{now_epoch, AppSyncData, SchemaState, SharedAppSyncState};

/// Every operation name in the AWS AppSync Smithy model (74 operations).
pub const APPSYNC_ACTIONS: &[&str] = &[
    "AssociateApi",
    "AssociateMergedGraphqlApi",
    "AssociateSourceGraphqlApi",
    "CreateApi",
    "CreateApiCache",
    "CreateApiKey",
    "CreateChannelNamespace",
    "CreateDataSource",
    "CreateDomainName",
    "CreateFunction",
    "CreateGraphqlApi",
    "CreateResolver",
    "CreateType",
    "DeleteApi",
    "DeleteApiCache",
    "DeleteApiKey",
    "DeleteChannelNamespace",
    "DeleteDataSource",
    "DeleteDomainName",
    "DeleteFunction",
    "DeleteGraphqlApi",
    "DeleteResolver",
    "DeleteType",
    "DisassociateApi",
    "DisassociateMergedGraphqlApi",
    "DisassociateSourceGraphqlApi",
    "EvaluateCode",
    "EvaluateMappingTemplate",
    "FlushApiCache",
    "GetApi",
    "GetApiAssociation",
    "GetApiCache",
    "GetChannelNamespace",
    "GetDataSource",
    "GetDataSourceIntrospection",
    "GetDomainName",
    "GetFunction",
    "GetGraphqlApi",
    "GetGraphqlApiEnvironmentVariables",
    "GetIntrospectionSchema",
    "GetResolver",
    "GetSchemaCreationStatus",
    "GetSourceApiAssociation",
    "GetType",
    "ListApiKeys",
    "ListApis",
    "ListChannelNamespaces",
    "ListDataSources",
    "ListDomainNames",
    "ListFunctions",
    "ListGraphqlApis",
    "ListResolvers",
    "ListResolversByFunction",
    "ListSourceApiAssociations",
    "ListTagsForResource",
    "ListTypes",
    "ListTypesByAssociation",
    "PutGraphqlApiEnvironmentVariables",
    "StartDataSourceIntrospection",
    "StartSchemaCreation",
    "StartSchemaMerge",
    "TagResource",
    "UntagResource",
    "UpdateApi",
    "UpdateApiCache",
    "UpdateApiKey",
    "UpdateChannelNamespace",
    "UpdateDataSource",
    "UpdateDomainName",
    "UpdateFunction",
    "UpdateGraphqlApi",
    "UpdateResolver",
    "UpdateSourceApiAssociation",
    "UpdateType",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
/// `GetSchemaCreationStatus` is included because reading it settles the async
/// schema-creation lifecycle from `Processing` to `Success`.
const SAVE_AFTER: &[&str] = &[
    "CreateApi",
    "CreateApiCache",
    "CreateApiKey",
    "CreateChannelNamespace",
    "CreateDataSource",
    "CreateDomainName",
    "CreateFunction",
    "CreateGraphqlApi",
    "CreateResolver",
    "CreateType",
    "DeleteApi",
    "DeleteApiCache",
    "DeleteApiKey",
    "DeleteChannelNamespace",
    "DeleteDataSource",
    "DeleteDomainName",
    "DeleteFunction",
    "DeleteGraphqlApi",
    "DeleteResolver",
    "DeleteType",
    "AssociateApi",
    "AssociateMergedGraphqlApi",
    "AssociateSourceGraphqlApi",
    "DisassociateApi",
    "DisassociateMergedGraphqlApi",
    "DisassociateSourceGraphqlApi",
    "FlushApiCache",
    "PutGraphqlApiEnvironmentVariables",
    "StartDataSourceIntrospection",
    "StartSchemaCreation",
    "StartSchemaMerge",
    "GetSchemaCreationStatus",
    "TagResource",
    "UntagResource",
    "UpdateApi",
    "UpdateApiCache",
    "UpdateApiKey",
    "UpdateChannelNamespace",
    "UpdateDataSource",
    "UpdateDomainName",
    "UpdateFunction",
    "UpdateGraphqlApi",
    "UpdateResolver",
    "UpdateSourceApiAssociation",
    "UpdateType",
];

pub struct AppSyncService {
    state: SharedAppSyncState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl AppSyncService {
    pub fn new(state: SharedAppSyncState) -> Self {
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
    /// method + `@http` URI template. Labels are captured in path order.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
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
        let v = |xs: &[&str]| xs.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            // ---- v2 Event API surface ----
            ["v2", "apis"] if post => ("CreateApi", vec![]),
            ["v2", "apis"] if get => ("ListApis", vec![]),
            ["v2", "apis", id] if get => ("GetApi", v(&[id])),
            ["v2", "apis", id] if post => ("UpdateApi", v(&[id])),
            ["v2", "apis", id] if del => ("DeleteApi", v(&[id])),
            ["v2", "apis", id, "channelNamespaces"] if post => ("CreateChannelNamespace", v(&[id])),
            ["v2", "apis", id, "channelNamespaces"] if get => ("ListChannelNamespaces", v(&[id])),
            ["v2", "apis", id, "channelNamespaces", n] if get => {
                ("GetChannelNamespace", v(&[id, n]))
            }
            ["v2", "apis", id, "channelNamespaces", n] if post => {
                ("UpdateChannelNamespace", v(&[id, n]))
            }
            ["v2", "apis", id, "channelNamespaces", n] if del => {
                ("DeleteChannelNamespace", v(&[id, n]))
            }

            // ---- v1 GraphQL API surface ----
            ["v1", "apis"] if post => ("CreateGraphqlApi", vec![]),
            ["v1", "apis"] if get => ("ListGraphqlApis", vec![]),

            // API caches (before the generic {apiId} arms; distinct trailing literal)
            ["v1", "apis", id, "ApiCaches", "update"] if post => ("UpdateApiCache", v(&[id])),
            ["v1", "apis", id, "ApiCaches"] if post => ("CreateApiCache", v(&[id])),
            ["v1", "apis", id, "ApiCaches"] if get => ("GetApiCache", v(&[id])),
            ["v1", "apis", id, "ApiCaches"] if del => ("DeleteApiCache", v(&[id])),
            ["v1", "apis", id, "FlushCache"] if del => ("FlushApiCache", v(&[id])),

            // API keys
            ["v1", "apis", id, "apikeys"] if post => ("CreateApiKey", v(&[id])),
            ["v1", "apis", id, "apikeys"] if get => ("ListApiKeys", v(&[id])),
            ["v1", "apis", id, "apikeys", k] if del => ("DeleteApiKey", v(&[id, k])),
            ["v1", "apis", id, "apikeys", k] if post => ("UpdateApiKey", v(&[id, k])),

            // Data sources
            ["v1", "apis", id, "datasources"] if post => ("CreateDataSource", v(&[id])),
            ["v1", "apis", id, "datasources"] if get => ("ListDataSources", v(&[id])),
            ["v1", "apis", id, "datasources", n] if get => ("GetDataSource", v(&[id, n])),
            ["v1", "apis", id, "datasources", n] if post => ("UpdateDataSource", v(&[id, n])),
            ["v1", "apis", id, "datasources", n] if del => ("DeleteDataSource", v(&[id, n])),

            // Functions
            ["v1", "apis", id, "functions"] if post => ("CreateFunction", v(&[id])),
            ["v1", "apis", id, "functions"] if get => ("ListFunctions", v(&[id])),
            ["v1", "apis", id, "functions", fid, "resolvers"] if get => {
                ("ListResolversByFunction", v(&[id, fid]))
            }
            ["v1", "apis", id, "functions", fid] if get => ("GetFunction", v(&[id, fid])),
            ["v1", "apis", id, "functions", fid] if post => ("UpdateFunction", v(&[id, fid])),
            ["v1", "apis", id, "functions", fid] if del => ("DeleteFunction", v(&[id, fid])),

            // Types + resolvers (resolvers nest under types)
            ["v1", "apis", id, "types", tn, "resolvers"] if post => {
                ("CreateResolver", v(&[id, tn]))
            }
            ["v1", "apis", id, "types", tn, "resolvers"] if get => ("ListResolvers", v(&[id, tn])),
            ["v1", "apis", id, "types", tn, "resolvers", fn_] if get => {
                ("GetResolver", v(&[id, tn, fn_]))
            }
            ["v1", "apis", id, "types", tn, "resolvers", fn_] if post => {
                ("UpdateResolver", v(&[id, tn, fn_]))
            }
            ["v1", "apis", id, "types", tn, "resolvers", fn_] if del => {
                ("DeleteResolver", v(&[id, tn, fn_]))
            }
            ["v1", "apis", id, "types"] if post => ("CreateType", v(&[id])),
            ["v1", "apis", id, "types"] if get => ("ListTypes", v(&[id])),
            ["v1", "apis", id, "types", tn] if get => ("GetType", v(&[id, tn])),
            ["v1", "apis", id, "types", tn] if post => ("UpdateType", v(&[id, tn])),
            ["v1", "apis", id, "types", tn] if del => ("DeleteType", v(&[id, tn])),

            // Schema
            ["v1", "apis", id, "schema"] if get => ("GetIntrospectionSchema", v(&[id])),
            ["v1", "apis", id, "schemacreation"] if post => ("StartSchemaCreation", v(&[id])),
            ["v1", "apis", id, "schemacreation"] if get => ("GetSchemaCreationStatus", v(&[id])),

            // Environment variables
            ["v1", "apis", id, "environmentVariables"] if get => {
                ("GetGraphqlApiEnvironmentVariables", v(&[id]))
            }
            ["v1", "apis", id, "environmentVariables"] if put => {
                ("PutGraphqlApiEnvironmentVariables", v(&[id]))
            }

            // Source-API associations listed on a graphql api
            ["v1", "apis", id, "sourceApiAssociations"] if get => {
                ("ListSourceApiAssociations", v(&[id]))
            }

            // Generic graphql api by id (must come after the sub-resource arms)
            ["v1", "apis", id] if get => ("GetGraphqlApi", v(&[id])),
            ["v1", "apis", id] if post => ("UpdateGraphqlApi", v(&[id])),
            ["v1", "apis", id] if del => ("DeleteGraphqlApi", v(&[id])),

            // Domain names + api associations
            ["v1", "domainnames"] if post => ("CreateDomainName", vec![]),
            ["v1", "domainnames"] if get => ("ListDomainNames", vec![]),
            ["v1", "domainnames", dn, "apiassociation"] if post => ("AssociateApi", v(&[dn])),
            ["v1", "domainnames", dn, "apiassociation"] if get => ("GetApiAssociation", v(&[dn])),
            ["v1", "domainnames", dn, "apiassociation"] if del => ("DisassociateApi", v(&[dn])),
            ["v1", "domainnames", dn] if get => ("GetDomainName", v(&[dn])),
            ["v1", "domainnames", dn] if post => ("UpdateDomainName", v(&[dn])),
            ["v1", "domainnames", dn] if del => ("DeleteDomainName", v(&[dn])),

            // Merged / source API associations
            ["v1", "sourceApis", sid, "mergedApiAssociations"] if post => {
                ("AssociateMergedGraphqlApi", v(&[sid]))
            }
            ["v1", "sourceApis", sid, "mergedApiAssociations", aid] if del => {
                ("DisassociateMergedGraphqlApi", v(&[sid, aid]))
            }
            ["v1", "mergedApis", mid, "sourceApiAssociations"] if post => {
                ("AssociateSourceGraphqlApi", v(&[mid]))
            }
            ["v1", "mergedApis", mid, "sourceApiAssociations", aid, "types"] if get => {
                ("ListTypesByAssociation", v(&[mid, aid]))
            }
            ["v1", "mergedApis", mid, "sourceApiAssociations", aid, "merge"] if post => {
                ("StartSchemaMerge", v(&[mid, aid]))
            }
            ["v1", "mergedApis", mid, "sourceApiAssociations", aid] if get => {
                ("GetSourceApiAssociation", v(&[mid, aid]))
            }
            ["v1", "mergedApis", mid, "sourceApiAssociations", aid] if post => {
                ("UpdateSourceApiAssociation", v(&[mid, aid]))
            }
            ["v1", "mergedApis", mid, "sourceApiAssociations", aid] if del => {
                ("DisassociateSourceGraphqlApi", v(&[mid, aid]))
            }

            // Data-source introspection
            ["v1", "datasources", "introspections"] if post => {
                ("StartDataSourceIntrospection", vec![])
            }
            ["v1", "datasources", "introspections", iid] if get => {
                ("GetDataSourceIntrospection", v(&[iid]))
            }

            // Evaluation data plane
            ["v1", "dataplane-evaluatecode"] if post => ("EvaluateCode", vec![]),
            ["v1", "dataplane-evaluatetemplate"] if post => ("EvaluateMappingTemplate", vec![]),

            // Tags
            ["v1", "tags", arn] if get => ("ListTagsForResource", v(&[arn])),
            ["v1", "tags", arn] if post => ("TagResource", v(&[arn])),
            ["v1", "tags", arn] if del => ("UntagResource", v(&[arn])),

            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for AppSyncService {
    fn service_name(&self) -> &str {
        "appsync"
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
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if SAVE_AFTER.contains(&action) && success {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        APPSYNC_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl AppSyncService {
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
        crate::validate::validate_query(action, &q)?;
        let l = |i: usize| labels.get(i).map(String::as_str).unwrap_or_default();
        match action {
            // ---- GraphQL APIs ----
            "CreateGraphqlApi" => self.create_graphql_api(&ctx, &body),
            "GetGraphqlApi" => self.get_graphql_api(&ctx, l(0)),
            "ListGraphqlApis" => self.list_graphql_apis(&ctx),
            "UpdateGraphqlApi" => self.update_graphql_api(&ctx, l(0), &body),
            "DeleteGraphqlApi" => self.delete_graphql_api(&ctx, l(0)),

            // ---- API keys ----
            "CreateApiKey" => self.create_api_key(&ctx, l(0), &body),
            "ListApiKeys" => self.list_api_keys(&ctx, l(0)),
            "UpdateApiKey" => self.update_api_key(&ctx, l(0), l(1), &body),
            "DeleteApiKey" => self.delete_api_key(&ctx, l(0), l(1)),

            // ---- Data sources ----
            "CreateDataSource" => self.create_data_source(&ctx, l(0), &body),
            "GetDataSource" => self.get_data_source(&ctx, l(0), l(1)),
            "ListDataSources" => self.list_data_sources(&ctx, l(0)),
            "UpdateDataSource" => self.update_data_source(&ctx, l(0), l(1), &body),
            "DeleteDataSource" => self.delete_data_source(&ctx, l(0), l(1)),

            // ---- Resolvers ----
            "CreateResolver" => self.create_resolver(&ctx, l(0), l(1), &body),
            "GetResolver" => self.get_resolver(&ctx, l(0), l(1), l(2)),
            "ListResolvers" => self.list_resolvers(&ctx, l(0), l(1)),
            "ListResolversByFunction" => self.list_resolvers_by_function(&ctx, l(0), l(1)),
            "UpdateResolver" => self.update_resolver(&ctx, l(0), l(1), l(2), &body),
            "DeleteResolver" => self.delete_resolver(&ctx, l(0), l(1), l(2)),

            // ---- Functions ----
            "CreateFunction" => self.create_function(&ctx, l(0), &body),
            "GetFunction" => self.get_function(&ctx, l(0), l(1)),
            "ListFunctions" => self.list_functions(&ctx, l(0)),
            "UpdateFunction" => self.update_function(&ctx, l(0), l(1), &body),
            "DeleteFunction" => self.delete_function(&ctx, l(0), l(1)),

            // ---- Types + schema ----
            "CreateType" => self.create_type(&ctx, l(0), &body),
            "GetType" => self.get_type(&ctx, l(0), l(1), &q),
            "ListTypes" => self.list_types(&ctx, l(0), &q),
            "UpdateType" => self.update_type(&ctx, l(0), l(1), &body),
            "DeleteType" => self.delete_type(&ctx, l(0), l(1)),
            "StartSchemaCreation" => self.start_schema_creation(&ctx, l(0), &body),
            "GetSchemaCreationStatus" => self.get_schema_creation_status(&ctx, l(0)),
            "GetIntrospectionSchema" => self.get_introspection_schema(&ctx, l(0), &q),

            // ---- API caches ----
            "CreateApiCache" => self.create_api_cache(&ctx, l(0), &body),
            "UpdateApiCache" => self.update_api_cache(&ctx, l(0), &body),
            "GetApiCache" => self.get_api_cache(&ctx, l(0)),
            "DeleteApiCache" => self.delete_api_cache(&ctx, l(0)),
            "FlushApiCache" => self.flush_api_cache(&ctx, l(0)),

            // ---- Environment variables ----
            "PutGraphqlApiEnvironmentVariables" => self.put_env_vars(&ctx, l(0), &body),
            "GetGraphqlApiEnvironmentVariables" => self.get_env_vars(&ctx, l(0)),

            // ---- Domain names + api associations ----
            "CreateDomainName" => self.create_domain_name(&ctx, &body),
            "GetDomainName" => self.get_domain_name(&ctx, l(0)),
            "ListDomainNames" => self.list_domain_names(&ctx),
            "UpdateDomainName" => self.update_domain_name(&ctx, l(0), &body),
            "DeleteDomainName" => self.delete_domain_name(&ctx, l(0)),
            "AssociateApi" => self.associate_api(&ctx, l(0), &body),
            "GetApiAssociation" => self.get_api_association(&ctx, l(0)),
            "DisassociateApi" => self.disassociate_api(&ctx, l(0)),

            // ---- Event APIs + channel namespaces ----
            "CreateApi" => self.create_api(&ctx, &body),
            "GetApi" => self.get_api(&ctx, l(0)),
            "ListApis" => self.list_apis(&ctx),
            "UpdateApi" => self.update_api(&ctx, l(0), &body),
            "DeleteApi" => self.delete_api(&ctx, l(0)),
            "CreateChannelNamespace" => self.create_channel_namespace(&ctx, l(0), &body),
            "GetChannelNamespace" => self.get_channel_namespace(&ctx, l(0), l(1)),
            "ListChannelNamespaces" => self.list_channel_namespaces(&ctx, l(0)),
            "UpdateChannelNamespace" => self.update_channel_namespace(&ctx, l(0), l(1), &body),
            "DeleteChannelNamespace" => self.delete_channel_namespace(&ctx, l(0), l(1)),

            // ---- Source API associations ----
            "AssociateSourceGraphqlApi" => self.associate_source_api(&ctx, l(0), &body),
            "AssociateMergedGraphqlApi" => self.associate_merged_api(&ctx, l(0), &body),
            "GetSourceApiAssociation" => self.get_source_api_association(&ctx, l(1)),
            "ListSourceApiAssociations" => self.list_source_api_associations(&ctx, l(0)),
            "UpdateSourceApiAssociation" => self.update_source_api_association(&ctx, l(1), &body),
            "DisassociateSourceGraphqlApi" => self.disassociate_source_api(&ctx, l(1)),
            "DisassociateMergedGraphqlApi" => self.disassociate_source_api(&ctx, l(1)),
            "ListTypesByAssociation" => self.list_types_by_association(&ctx, l(0), l(1), &q),
            "StartSchemaMerge" => self.start_schema_merge(&ctx, l(1)),

            // ---- Data-source introspection ----
            "StartDataSourceIntrospection" => self.start_ds_introspection(&ctx, &body),
            "GetDataSourceIntrospection" => self.get_ds_introspection(&ctx, l(0)),

            // ---- Tags ----
            "TagResource" => self.tag_resource(&ctx, l(0), &body),
            "UntagResource" => self.untag_resource(&ctx, l(0), &q),
            "ListTagsForResource" => self.list_tags_for_resource(&ctx, l(0)),

            // ---- Evaluation data plane ----
            "EvaluateCode" => Self::evaluate_code(&body),
            "EvaluateMappingTemplate" => Self::evaluate_template(&body),

            _ => Err(AwsServiceError::action_not_implemented("appsync", action)),
        }
    }

    // ===================== GraphQL APIs =====================

    fn create_graphql_api(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let api_id = gen_api_id();
        let api = self.build_graphql_api(ctx, &api_id, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(tags) = body.get("tags").and_then(Value::as_object) {
            let arn = graphql_api_arn(&ctx.region, &ctx.account, &api_id);
            data.tags.insert(api_id.clone(), string_map(tags));
            data.tags.insert(arn, string_map(tags));
        }
        data.graphql_apis.insert(api_id, api.clone());
        Ok(ok(json!({ "graphqlApi": api })))
    }

    fn build_graphql_api(&self, ctx: &Ctx, api_id: &str, body: &Value) -> Value {
        let arn = graphql_api_arn(&ctx.region, &ctx.account, api_id);
        let mut api = Map::new();
        api.insert("apiId".into(), json!(api_id));
        api.insert("arn".into(), json!(arn));
        api.insert("owner".into(), json!(ctx.account));
        api.insert(
            "uris".into(),
            json!({
                "GRAPHQL": format!("https://{api_id}.appsync-api.{}.amazonaws.com/graphql", ctx.region),
                "REALTIME": format!("wss://{api_id}.appsync-realtime-api.{}.amazonaws.com/graphql", ctx.region),
            }),
        );
        api.insert(
            "dns".into(),
            json!({
                "GRAPHQL": format!("{api_id}.appsync-api.{}.amazonaws.com", ctx.region),
                "REALTIME": format!("{api_id}.appsync-realtime-api.{}.amazonaws.com", ctx.region),
            }),
        );
        // Defaults for members AWS always populates.
        api.insert("apiType".into(), json!("GRAPHQL"));
        api.insert("visibility".into(), json!("GLOBAL"));
        api.insert("xrayEnabled".into(), json!(false));
        copy_keys(
            &mut api,
            body,
            &[
                "name",
                "authenticationType",
                "logConfig",
                "userPoolConfig",
                "openIDConnectConfig",
                "additionalAuthenticationProviders",
                "xrayEnabled",
                "lambdaAuthorizerConfig",
                "apiType",
                "mergedApiExecutionRoleArn",
                "visibility",
                "ownerContact",
                "introspectionConfig",
                "queryDepthLimit",
                "resolverCountLimit",
                "enhancedMetricsConfig",
                "tags",
            ],
        );
        Value::Object(api)
    }

    fn get_graphql_api(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let guard = self.state.read();
        let api = guard
            .get(&ctx.account)
            .and_then(|d| d.graphql_apis.get(api_id))
            .cloned()
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        Ok(ok(json!({ "graphqlApi": api })))
    }

    fn list_graphql_apis(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let apis: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.graphql_apis.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "graphqlApis": apis })))
    }

    fn update_graphql_api(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.graphql_apis.contains_key(api_id) {
            return Err(not_found(&format!("GraphQL API {api_id} not found.")));
        }
        // Rebuild preserving apiId/arn/uris identity, applying the update body.
        let api = self.build_graphql_api(ctx, api_id, body);
        data.graphql_apis.insert(api_id.to_string(), api.clone());
        Ok(ok(json!({ "graphqlApi": api })))
    }

    fn delete_graphql_api(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.graphql_apis.remove(api_id).is_none() {
            return Err(not_found(&format!("GraphQL API {api_id} not found.")));
        }
        data.api_keys.remove(api_id);
        data.data_sources.remove(api_id);
        data.resolvers.remove(api_id);
        data.functions.remove(api_id);
        data.types.remove(api_id);
        data.api_caches.remove(api_id);
        data.schemas.remove(api_id);
        data.env_vars.remove(api_id);
        Ok(ok(json!({})))
    }

    /// Return `NotFoundException` unless the GraphQL API exists in this account.
    fn require_api(&self, data: &AppSyncData, api_id: &str) -> Result<(), AwsServiceError> {
        require_label(api_id, "apiId")?;
        if data.graphql_apis.contains_key(api_id) {
            Ok(())
        } else {
            Err(not_found(&format!("GraphQL API {api_id} not found.")))
        }
    }

    // ===================== API keys =====================

    fn create_api_key(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let id = gen_hex(8);
        let expires = body
            .get("expires")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| now_epoch() as i64 + 7 * 24 * 3600);
        let mut key = Map::new();
        key.insert("id".into(), json!(id));
        key.insert("expires".into(), json!(expires));
        key.insert("deletes".into(), json!(expires + 60 * 24 * 3600));
        copy_keys(&mut key, body, &["description"]);
        let key = Value::Object(key);
        data.api_keys
            .entry(api_id.to_string())
            .or_default()
            .insert(id, key.clone());
        Ok(ok(json!({ "apiKey": key })))
    }

    fn list_api_keys(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let keys: Vec<Value> = data
            .api_keys
            .get(api_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "apiKeys": keys })))
    }

    fn update_api_key(
        &self,
        ctx: &Ctx,
        api_id: &str,
        id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let key = data
            .api_keys
            .get_mut(api_id)
            .and_then(|m| m.get_mut(id))
            .ok_or_else(|| not_found(&format!("API key {id} not found.")))?;
        if let Some(obj) = key.as_object_mut() {
            if let Some(d) = body.get("description") {
                obj.insert("description".into(), d.clone());
            }
            if let Some(e) = body.get("expires") {
                obj.insert("expires".into(), e.clone());
            }
        }
        Ok(ok(json!({ "apiKey": key })))
    }

    fn delete_api_key(
        &self,
        ctx: &Ctx,
        api_id: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(id, "id")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        match data.api_keys.get_mut(api_id).and_then(|m| m.remove(id)) {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found(&format!("API key {id} not found."))),
        }
    }

    // ===================== Data sources =====================

    fn create_data_source(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_body_str(body, "name")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let ds = build_data_source(ctx, api_id, &name, body);
        data.data_sources
            .entry(api_id.to_string())
            .or_default()
            .insert(name, ds.clone());
        Ok(ok(json!({ "dataSource": ds })))
    }

    fn get_data_source(
        &self,
        ctx: &Ctx,
        api_id: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(name, "name")?;
        let guard = self.state.read();
        let ds = guard
            .get(&ctx.account)
            .and_then(|d| d.data_sources.get(api_id))
            .and_then(|m| m.get(name))
            .cloned()
            .ok_or_else(|| not_found(&format!("Data source {name} not found.")))?;
        Ok(ok(json!({ "dataSource": ds })))
    }

    fn list_data_sources(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let items: Vec<Value> = data
            .data_sources
            .get(api_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "dataSources": items })))
    }

    fn update_data_source(
        &self,
        ctx: &Ctx,
        api_id: &str,
        name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(name, "name")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        if !data
            .data_sources
            .get(api_id)
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
        {
            return Err(not_found(&format!("Data source {name} not found.")));
        }
        let ds = build_data_source(ctx, api_id, name, body);
        data.data_sources
            .entry(api_id.to_string())
            .or_default()
            .insert(name.to_string(), ds.clone());
        Ok(ok(json!({ "dataSource": ds })))
    }

    fn delete_data_source(
        &self,
        ctx: &Ctx,
        api_id: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(name, "name")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        match data
            .data_sources
            .get_mut(api_id)
            .and_then(|m| m.remove(name))
        {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found(&format!("Data source {name} not found."))),
        }
    }

    // ===================== Resolvers =====================

    fn create_resolver(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        let field = require_body_str(body, "fieldName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let resolver = build_resolver(ctx, api_id, type_name, &field, body);
        data.resolvers
            .entry(api_id.to_string())
            .or_default()
            .insert(resolver_key(type_name, &field), resolver.clone());
        Ok(ok(json!({ "resolver": resolver })))
    }

    fn get_resolver(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
        field: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        require_label(field, "fieldName")?;
        let guard = self.state.read();
        let resolver = guard
            .get(&ctx.account)
            .and_then(|d| d.resolvers.get(api_id))
            .and_then(|m| m.get(&resolver_key(type_name, field)))
            .cloned()
            .ok_or_else(|| not_found(&format!("Resolver {type_name}.{field} not found.")))?;
        Ok(ok(json!({ "resolver": resolver })))
    }

    fn list_resolvers(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let prefix = format!("{type_name}::");
        let items: Vec<Value> = data
            .resolvers
            .get(api_id)
            .map(|m| {
                m.iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(_, v)| v.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(ok(json!({ "resolvers": items })))
    }

    fn list_resolvers_by_function(
        &self,
        ctx: &Ctx,
        api_id: &str,
        function_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(function_id, "functionId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let items: Vec<Value> = data
            .resolvers
            .get(api_id)
            .map(|m| {
                m.values()
                    .filter(|r| {
                        r.get("pipelineConfig")
                            .and_then(|p| p.get("functions"))
                            .and_then(Value::as_array)
                            .map(|fs| fs.iter().any(|f| f.as_str() == Some(function_id)))
                            .unwrap_or(false)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Ok(ok(json!({ "resolvers": items })))
    }

    fn update_resolver(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
        field: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        require_label(field, "fieldName")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let key = resolver_key(type_name, field);
        if !data
            .resolvers
            .get(api_id)
            .map(|m| m.contains_key(&key))
            .unwrap_or(false)
        {
            return Err(not_found(&format!(
                "Resolver {type_name}.{field} not found."
            )));
        }
        let resolver = build_resolver(ctx, api_id, type_name, field, body);
        data.resolvers
            .entry(api_id.to_string())
            .or_default()
            .insert(key, resolver.clone());
        Ok(ok(json!({ "resolver": resolver })))
    }

    fn delete_resolver(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
        field: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        require_label(field, "fieldName")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        match data
            .resolvers
            .get_mut(api_id)
            .and_then(|m| m.remove(&resolver_key(type_name, field)))
        {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found(&format!(
                "Resolver {type_name}.{field} not found."
            ))),
        }
    }

    // ===================== Functions =====================

    fn create_function(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let function_id = gen_hex(12);
        let func = build_function(ctx, api_id, &function_id, body);
        data.functions
            .entry(api_id.to_string())
            .or_default()
            .insert(function_id, func.clone());
        Ok(ok(json!({ "functionConfiguration": func })))
    }

    fn get_function(
        &self,
        ctx: &Ctx,
        api_id: &str,
        function_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(function_id, "functionId")?;
        let guard = self.state.read();
        let func = guard
            .get(&ctx.account)
            .and_then(|d| d.functions.get(api_id))
            .and_then(|m| m.get(function_id))
            .cloned()
            .ok_or_else(|| not_found(&format!("Function {function_id} not found.")))?;
        Ok(ok(json!({ "functionConfiguration": func })))
    }

    fn list_functions(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let items: Vec<Value> = data
            .functions
            .get(api_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "functions": items })))
    }

    fn update_function(
        &self,
        ctx: &Ctx,
        api_id: &str,
        function_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(function_id, "functionId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        if !data
            .functions
            .get(api_id)
            .map(|m| m.contains_key(function_id))
            .unwrap_or(false)
        {
            return Err(not_found(&format!("Function {function_id} not found.")));
        }
        let func = build_function(ctx, api_id, function_id, body);
        data.functions
            .entry(api_id.to_string())
            .or_default()
            .insert(function_id.to_string(), func.clone());
        Ok(ok(json!({ "functionConfiguration": func })))
    }

    fn delete_function(
        &self,
        ctx: &Ctx,
        api_id: &str,
        function_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(function_id, "functionId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        match data
            .functions
            .get_mut(api_id)
            .and_then(|m| m.remove(function_id))
        {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found(&format!("Function {function_id} not found."))),
        }
    }

    // ===================== Types + schema =====================

    fn create_type(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let definition = require_body_str(body, "definition")?.to_string();
        let format = require_body_str(body, "format")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let name = parse_type_name(&definition).unwrap_or_else(|| gen_hex(6));
        let ty = build_type(ctx, api_id, &name, &definition, &format);
        data.types
            .entry(api_id.to_string())
            .or_default()
            .insert(name, ty.clone());
        Ok(ok(json!({ "type": ty })))
    }

    fn get_type(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        require_query(q, "format")?;
        let guard = self.state.read();
        let ty = guard
            .get(&ctx.account)
            .and_then(|d| d.types.get(api_id))
            .and_then(|m| m.get(type_name))
            .cloned()
            .ok_or_else(|| not_found(&format!("Type {type_name} not found.")))?;
        Ok(ok(json!({ "type": ty })))
    }

    fn list_types(
        &self,
        ctx: &Ctx,
        api_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_query(q, "format")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let items: Vec<Value> = data
            .types
            .get(api_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "types": items })))
    }

    fn update_type(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        let format = require_body_str(body, "format")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let existing = data
            .types
            .get(api_id)
            .and_then(|m| m.get(type_name))
            .cloned()
            .ok_or_else(|| not_found(&format!("Type {type_name} not found.")))?;
        let definition = body
            .get("definition")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                existing
                    .get("definition")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        let ty = build_type(ctx, api_id, type_name, &definition, &format);
        data.types
            .entry(api_id.to_string())
            .or_default()
            .insert(type_name.to_string(), ty.clone());
        Ok(ok(json!({ "type": ty })))
    }

    fn delete_type(
        &self,
        ctx: &Ctx,
        api_id: &str,
        type_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(type_name, "typeName")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        match data.types.get_mut(api_id).and_then(|m| m.remove(type_name)) {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found(&format!("Type {type_name} not found."))),
        }
    }

    fn start_schema_creation(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let def_b64 = require_body_str(body, "definition")?;
        let sdl = decode_blob(def_b64);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        data.schemas.insert(
            api_id.to_string(),
            SchemaState {
                definition: sdl,
                status: "Processing".to_string(),
                details: "Schema creation in progress.".to_string(),
            },
        );
        Ok(ok(json!({ "status": "PROCESSING" })))
    }

    fn get_schema_creation_status(
        &self,
        ctx: &Ctx,
        api_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let schema = data.schemas.entry(api_id.to_string()).or_default();
        // Settle the async lifecycle: first status read after creation flips
        // Processing -> Success, mirroring other async ops.
        if schema.status.is_empty() || schema.status == "NotApplicable" {
            schema.status = "NotApplicable".to_string();
        } else {
            schema.status = "Success".to_string();
            schema.details = "Schema is ready.".to_string();
        }
        Ok(ok(json!({
            "status": schema_status_wire(&schema.status),
            "details": schema.details,
        })))
    }

    fn get_introspection_schema(
        &self,
        ctx: &Ctx,
        api_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let format = require_query(q, "format")?.to_string();
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let schema = data.schemas.get(api_id).map(|s| s.definition.clone());
        match schema {
            Some(sdl) if !sdl.is_empty() => {
                let bytes = crate::schema::introspection_bytes(&sdl, &format);
                let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
                Ok(ok(json!({ "schema": b64 })))
            }
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GraphQLSchemaException",
                "No schema found. Provide a valid schema via StartSchemaCreation first.",
            )),
        }
    }

    // ===================== API caches =====================

    fn create_api_cache(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        if data.api_caches.contains_key(api_id) {
            return Err(bad("An API cache already exists for this API."));
        }
        let cache = build_api_cache(body);
        data.api_caches.insert(api_id.to_string(), cache.clone());
        Ok(ok(json!({ "apiCache": cache })))
    }

    fn update_api_cache(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        if !data.api_caches.contains_key(api_id) {
            return Err(not_found("API cache not found."));
        }
        let cache = build_api_cache(body);
        data.api_caches.insert(api_id.to_string(), cache.clone());
        Ok(ok(json!({ "apiCache": cache })))
    }

    fn get_api_cache(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let guard = self.state.read();
        let cache = guard
            .get(&ctx.account)
            .and_then(|d| d.api_caches.get(api_id))
            .cloned()
            .ok_or_else(|| not_found("API cache not found."))?;
        Ok(ok(json!({ "apiCache": cache })))
    }

    fn delete_api_cache(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        match data.api_caches.remove(api_id) {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found("API cache not found.")),
        }
    }

    fn flush_api_cache(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found("API cache not found."))?;
        if data.api_caches.contains_key(api_id) {
            Ok(ok(json!({})))
        } else {
            Err(not_found("API cache not found."))
        }
    }

    // ===================== Environment variables =====================

    fn put_env_vars(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        self.require_api(data, api_id)?;
        let vars = body
            .get("environmentVariables")
            .and_then(Value::as_object)
            .map(string_map)
            .unwrap_or_default();
        data.env_vars.insert(api_id.to_string(), vars.clone());
        Ok(ok(json!({ "environmentVariables": vars })))
    }

    fn get_env_vars(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("GraphQL API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let vars = data.env_vars.get(api_id).cloned().unwrap_or_default();
        Ok(ok(json!({ "environmentVariables": vars })))
    }

    // ===================== Domain names =====================

    fn create_domain_name(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let domain = require_body_str(body, "domainName")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let cfg = build_domain_name(ctx, &domain, body);
        data.domain_names.insert(domain, cfg.clone());
        Ok(ok(json!({ "domainNameConfig": cfg })))
    }

    fn get_domain_name(&self, ctx: &Ctx, domain: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(domain, "domainName")?;
        let guard = self.state.read();
        let cfg = guard
            .get(&ctx.account)
            .and_then(|d| d.domain_names.get(domain))
            .cloned()
            .ok_or_else(|| not_found(&format!("Domain name {domain} not found.")))?;
        Ok(ok(json!({ "domainNameConfig": cfg })))
    }

    fn list_domain_names(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let items: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.domain_names.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "domainNameConfigs": items })))
    }

    fn update_domain_name(
        &self,
        ctx: &Ctx,
        domain: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(domain, "domainName")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let mut cfg = data
            .domain_names
            .get(domain)
            .cloned()
            .ok_or_else(|| not_found(&format!("Domain name {domain} not found.")))?;
        if let (Some(obj), Some(desc)) = (cfg.as_object_mut(), body.get("description")) {
            obj.insert("description".into(), desc.clone());
        }
        data.domain_names.insert(domain.to_string(), cfg.clone());
        Ok(ok(json!({ "domainNameConfig": cfg })))
    }

    fn delete_domain_name(&self, ctx: &Ctx, domain: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(domain, "domainName")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data.domain_names.remove(domain) {
            Some(_) => {
                data.api_associations.remove(domain);
                Ok(ok(json!({})))
            }
            None => Err(not_found(&format!("Domain name {domain} not found."))),
        }
    }

    fn associate_api(
        &self,
        ctx: &Ctx,
        domain: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(domain, "domainName")?;
        let api_id = require_body_str(body, "apiId")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.domain_names.contains_key(domain) {
            return Err(not_found(&format!("Domain name {domain} not found.")));
        }
        let assoc = json!({
            "domainName": domain,
            "apiId": api_id,
            "associationStatus": "SUCCESS",
        });
        data.api_associations
            .insert(domain.to_string(), assoc.clone());
        Ok(ok(json!({ "apiAssociation": assoc })))
    }

    fn get_api_association(&self, ctx: &Ctx, domain: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(domain, "domainName")?;
        let guard = self.state.read();
        let assoc = guard
            .get(&ctx.account)
            .and_then(|d| d.api_associations.get(domain))
            .cloned()
            .ok_or_else(|| not_found(&format!("No API association for {domain}.")))?;
        Ok(ok(json!({ "apiAssociation": assoc })))
    }

    fn disassociate_api(&self, ctx: &Ctx, domain: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(domain, "domainName")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data.api_associations.remove(domain) {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found(&format!("No API association for {domain}."))),
        }
    }

    // ===================== Event APIs =====================

    fn create_api(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let api_id = gen_api_id();
        let api = build_event_api(ctx, &api_id, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.apis.insert(api_id, api.clone());
        Ok(ok(json!({ "api": api })))
    }

    fn get_api(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let guard = self.state.read();
        let api = guard
            .get(&ctx.account)
            .and_then(|d| d.apis.get(api_id))
            .cloned()
            .ok_or_else(|| not_found(&format!("API {api_id} not found.")))?;
        Ok(ok(json!({ "api": api })))
    }

    fn list_apis(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let items: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.apis.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "apis": items })))
    }

    fn update_api(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.apis.contains_key(api_id) {
            return Err(not_found(&format!("API {api_id} not found.")));
        }
        let api = build_event_api(ctx, api_id, body);
        data.apis.insert(api_id.to_string(), api.clone());
        Ok(ok(json!({ "api": api })))
    }

    fn delete_api(&self, ctx: &Ctx, api_id: &str) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data.apis.remove(api_id) {
            Some(_) => {
                data.channel_namespaces.remove(api_id);
                Ok(ok(json!({})))
            }
            None => Err(not_found(&format!("API {api_id} not found."))),
        }
    }

    fn create_channel_namespace(
        &self,
        ctx: &Ctx,
        api_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = require_body_str(body, "name")?.to_string();
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.apis.contains_key(api_id) {
            return Err(not_found(&format!("API {api_id} not found.")));
        }
        if data
            .channel_namespaces
            .get(api_id)
            .map(|m| m.contains_key(&name))
            .unwrap_or(false)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ConflictException",
                format!("Channel namespace {name} already exists."),
            ));
        }
        let ns = build_channel_namespace(ctx, api_id, &name, body);
        data.channel_namespaces
            .entry(api_id.to_string())
            .or_default()
            .insert(name, ns.clone());
        Ok(ok(json!({ "channelNamespace": ns })))
    }

    fn get_channel_namespace(
        &self,
        ctx: &Ctx,
        api_id: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(name, "name")?;
        let guard = self.state.read();
        let ns = guard
            .get(&ctx.account)
            .and_then(|d| d.channel_namespaces.get(api_id))
            .and_then(|m| m.get(name))
            .cloned()
            .ok_or_else(|| not_found(&format!("Channel namespace {name} not found.")))?;
        Ok(ok(json!({ "channelNamespace": ns })))
    }

    fn list_channel_namespaces(
        &self,
        ctx: &Ctx,
        api_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("API {api_id} not found.")))?;
        if !data.apis.contains_key(api_id) {
            return Err(not_found(&format!("API {api_id} not found.")));
        }
        let items: Vec<Value> = data
            .channel_namespaces
            .get(api_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "channelNamespaces": items })))
    }

    fn update_channel_namespace(
        &self,
        ctx: &Ctx,
        api_id: &str,
        name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(name, "name")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data
            .channel_namespaces
            .get(api_id)
            .map(|m| m.contains_key(name))
            .unwrap_or(false)
        {
            return Err(not_found(&format!("Channel namespace {name} not found.")));
        }
        let ns = build_channel_namespace(ctx, api_id, name, body);
        data.channel_namespaces
            .entry(api_id.to_string())
            .or_default()
            .insert(name.to_string(), ns.clone());
        Ok(ok(json!({ "channelNamespace": ns })))
    }

    fn delete_channel_namespace(
        &self,
        ctx: &Ctx,
        api_id: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(name, "name")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data
            .channel_namespaces
            .get_mut(api_id)
            .and_then(|m| m.remove(name))
        {
            Some(_) => Ok(ok(json!({}))),
            None => Err(not_found(&format!("Channel namespace {name} not found."))),
        }
    }

    // ===================== Source API associations =====================

    fn associate_source_api(
        &self,
        ctx: &Ctx,
        merged_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(merged_id, "mergedApiIdentifier")?;
        let source_id = require_body_str(body, "sourceApiIdentifier")?.to_string();
        self.create_source_assoc(ctx, merged_id, &source_id, body)
    }

    fn associate_merged_api(
        &self,
        ctx: &Ctx,
        source_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(source_id, "sourceApiIdentifier")?;
        let merged_id = require_body_str(body, "mergedApiIdentifier")?.to_string();
        self.create_source_assoc(ctx, &merged_id, source_id, body)
    }

    fn create_source_assoc(
        &self,
        ctx: &Ctx,
        merged_id: &str,
        source_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        {
            // Both the merged and source APIs must exist. Synthetic ids (and an
            // unsubstituted `{...}` label) resolve to nothing -> NotFoundException,
            // which every association op declares.
            let guard = self.state.read();
            let data = guard.get(&ctx.account);
            if !api_ref_exists(data, merged_id) {
                return Err(not_found(&format!("Merged API {merged_id} not found.")));
            }
            if !api_ref_exists(data, source_id) {
                return Err(not_found(&format!("Source API {source_id} not found.")));
            }
        }
        let assoc_id = gen_hex(16);
        let mut assoc = Map::new();
        assoc.insert("associationId".into(), json!(assoc_id));
        assoc.insert(
            "associationArn".into(),
            json!(format!(
                "arn:aws:appsync:{}:{}:apis/{merged_id}/sourceApiAssociations/{assoc_id}",
                ctx.region, ctx.account
            )),
        );
        assoc.insert("sourceApiId".into(), json!(source_id));
        assoc.insert("mergedApiId".into(), json!(merged_id));
        assoc.insert("sourceApiAssociationStatus".into(), json!("MERGE_SUCCESS"));
        copy_keys(
            &mut assoc,
            body,
            &["description", "sourceApiAssociationConfig"],
        );
        let assoc = Value::Object(assoc);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.source_api_associations.insert(assoc_id, assoc.clone());
        Ok(ok(json!({ "sourceApiAssociation": assoc })))
    }

    fn get_source_api_association(
        &self,
        ctx: &Ctx,
        assoc_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(assoc_id, "associationId")?;
        let guard = self.state.read();
        let assoc = guard
            .get(&ctx.account)
            .and_then(|d| d.source_api_associations.get(assoc_id))
            .cloned()
            .ok_or_else(|| not_found(&format!("Association {assoc_id} not found.")))?;
        Ok(ok(json!({ "sourceApiAssociation": assoc })))
    }

    fn list_source_api_associations(
        &self,
        ctx: &Ctx,
        api_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(api_id, "apiId")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .ok_or_else(|| not_found(&format!("Merged API {api_id} not found.")))?;
        self.require_api(data, api_id)?;
        let items: Vec<Value> = data
            .source_api_associations
            .values()
            .filter(|a| {
                a.get("mergedApiId").and_then(Value::as_str) == Some(api_id)
                    || a.get("sourceApiId").and_then(Value::as_str) == Some(api_id)
            })
            .map(|a| {
                json!({
                    "associationId": a.get("associationId").cloned().unwrap_or(Value::Null),
                    "associationArn": a.get("associationArn").cloned().unwrap_or(Value::Null),
                    "sourceApiId": a.get("sourceApiId").cloned().unwrap_or(Value::Null),
                    "mergedApiId": a.get("mergedApiId").cloned().unwrap_or(Value::Null),
                    "description": a.get("description").cloned().unwrap_or(Value::Null),
                })
            })
            .collect();
        Ok(ok(json!({ "sourceApiAssociationSummaries": items })))
    }

    fn list_types_by_association(
        &self,
        ctx: &Ctx,
        merged_id: &str,
        assoc_id: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(merged_id, "mergedApiIdentifier")?;
        require_label(assoc_id, "associationId")?;
        require_query(q, "format")?;
        let guard = self.state.read();
        let data = guard
            .get(&ctx.account)
            .filter(|d| d.source_api_associations.contains_key(assoc_id))
            .ok_or_else(|| not_found(&format!("Association {assoc_id} not found.")))?;
        // Types merged into the merged API by StartSchemaMerge. Empty until a
        // merge has run for this association.
        let items: Vec<Value> = data
            .association_types
            .get(assoc_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        Ok(ok(json!({ "types": items })))
    }

    fn update_source_api_association(
        &self,
        ctx: &Ctx,
        assoc_id: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(assoc_id, "associationId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let assoc = data
            .source_api_associations
            .get_mut(assoc_id)
            .ok_or_else(|| not_found(&format!("Association {assoc_id} not found.")))?;
        if let Some(obj) = assoc.as_object_mut() {
            for k in ["description", "sourceApiAssociationConfig"] {
                if let Some(v) = body.get(k) {
                    obj.insert(k.to_string(), v.clone());
                }
            }
        }
        Ok(ok(json!({ "sourceApiAssociation": assoc })))
    }

    fn disassociate_source_api(
        &self,
        ctx: &Ctx,
        assoc_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(assoc_id, "associationId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        match data.source_api_associations.remove(assoc_id) {
            Some(_) => Ok(ok(
                json!({ "sourceApiAssociationStatus": "DELETION_IN_PROGRESS" }),
            )),
            None => Err(not_found(&format!("Association {assoc_id} not found."))),
        }
    }

    fn start_schema_merge(
        &self,
        ctx: &Ctx,
        assoc_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(assoc_id, "associationId")?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        // Resolve the source + merged APIs from the association before mutating.
        let (source_id, merged_id) = {
            let assoc = data
                .source_api_associations
                .get(assoc_id)
                .ok_or_else(|| not_found(&format!("Association {assoc_id} not found.")))?;
            let source_id = assoc
                .get("sourceApiId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let merged_id = assoc
                .get("mergedApiId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            (source_id, merged_id)
        };
        // Copy the source API's types into both the association's merged-type set
        // (returned by ListTypesByAssociation) and the merged API's own type
        // store, so the merge is observable rather than a no-op.
        let source_types: Vec<(String, Value)> = data
            .types
            .get(&source_id)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let merged_types = data
            .association_types
            .entry(assoc_id.to_string())
            .or_default();
        for (name, ty) in &source_types {
            merged_types.insert(name.clone(), ty.clone());
        }
        if !merged_id.is_empty() {
            let dest = data.types.entry(merged_id).or_default();
            for (name, ty) in &source_types {
                dest.insert(name.clone(), ty.clone());
            }
        }
        // Reflect the completed synchronous merge on the association record.
        if let Some(assoc) = data
            .source_api_associations
            .get_mut(assoc_id)
            .and_then(Value::as_object_mut)
        {
            assoc.insert("sourceApiAssociationStatus".into(), json!("MERGE_SUCCESS"));
        }
        Ok(ok(json!({ "sourceApiAssociationStatus": "MERGE_SUCCESS" })))
    }

    // ===================== Data-source introspection =====================

    fn start_ds_introspection(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = gen_hex(20);
        // The emulator has no live RDS instance to introspect, so the discovered
        // schema is an empty-but-well-formed IntrospectionResult. The record is
        // persisted so the Start -> Get poll flow retrieves it instead of a
        // permanent 404. The RDS config the caller supplied is echoed back on the
        // stored record so a reader can see what was requested.
        let rds_config = body.get("rdsDataApiConfig").cloned().unwrap_or(Value::Null);
        let record = json!({
            "introspectionId": id,
            "introspectionStatus": "SUCCESS",
            "introspectionStatusDetail": "Introspection completed.",
            "introspectionResult": {
                "models": [],
            },
            // Retained for reads/introspection; not part of the Get output shape.
            "rdsDataApiConfig": rds_config,
        });
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.introspections.insert(id.clone(), record);
        Ok(ok(json!({
            "introspectionId": id,
            "introspectionStatus": "SUCCESS",
            "introspectionStatusDetail": "Introspection completed.",
        })))
    }

    fn get_ds_introspection(
        &self,
        ctx: &Ctx,
        introspection_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_label(introspection_id, "introspectionId")?;
        let guard = self.state.read();
        let record = guard
            .get(&ctx.account)
            .and_then(|d| d.introspections.get(introspection_id))
            .ok_or_else(|| not_found(&format!("Introspection {introspection_id} not found.")))?;
        Ok(ok(json!({
            "introspectionId": record.get("introspectionId").cloned().unwrap_or(Value::Null),
            "introspectionStatus": record
                .get("introspectionStatus")
                .cloned()
                .unwrap_or(Value::Null),
            "introspectionStatusDetail": record
                .get("introspectionStatusDetail")
                .cloned()
                .unwrap_or(Value::Null),
            "introspectionResult": record
                .get("introspectionResult")
                .cloned()
                .unwrap_or(json!({ "models": [] })),
        })))
    }

    // ===================== Tags =====================

    fn tag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_resource_arn(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let entry = data.tags.entry(arn.to_string()).or_default();
        if let Some(tags) = body.get("tags").and_then(Value::as_object) {
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        Ok(ok(json!({})))
    }

    fn untag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        require_resource_arn(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(arn) {
            for (_, v) in q.iter().filter(|(k, _)| k == "tagKeys") {
                entry.remove(v);
            }
        }
        Ok(ok(json!({})))
    }

    fn list_tags_for_resource(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        require_resource_arn(arn)?;
        let guard = self.state.read();
        let tags = guard
            .get(&ctx.account)
            .and_then(|d| d.tags.get(arn))
            .cloned()
            .unwrap_or_default();
        Ok(ok(json!({ "tags": tags })))
    }

    // ===================== Evaluation data plane =====================

    fn evaluate_code(body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let code = body.get("code").and_then(Value::as_str).unwrap_or_default();
        let context = body
            .get("context")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let ev = crate::evaluate::evaluate(code, context);
        Ok(ok(build_evaluation(ev)))
    }

    fn evaluate_template(body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let template = body
            .get("template")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let context = body
            .get("context")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let ev = crate::evaluate::evaluate(template, context);
        Ok(ok(build_evaluation(ev)))
    }
}

// ===================== builders =====================

fn build_evaluation(ev: crate::evaluate::Evaluation) -> Value {
    let mut out = Map::new();
    if let Some(r) = ev.result {
        out.insert("evaluationResult".into(), json!(r));
    }
    if let Some(s) = ev.stash {
        out.insert("stash".into(), json!(s));
    }
    if let Some(e) = ev.error {
        out.insert("error".into(), json!({ "message": e }));
    }
    out.insert("logs".into(), json!([]));
    Value::Object(out)
}

fn build_data_source(ctx: &Ctx, api_id: &str, name: &str, body: &Value) -> Value {
    let mut ds = Map::new();
    ds.insert(
        "dataSourceArn".into(),
        json!(format!(
            "arn:aws:appsync:{}:{}:apis/{api_id}/datasources/{name}",
            ctx.region, ctx.account
        )),
    );
    ds.insert("name".into(), json!(name));
    copy_keys(
        &mut ds,
        body,
        &[
            "description",
            "type",
            "serviceRoleArn",
            "dynamodbConfig",
            "lambdaConfig",
            "elasticsearchConfig",
            "openSearchServiceConfig",
            "httpConfig",
            "relationalDatabaseConfig",
            "eventBridgeConfig",
            "metricsConfig",
        ],
    );
    Value::Object(ds)
}

fn build_resolver(ctx: &Ctx, api_id: &str, type_name: &str, field: &str, body: &Value) -> Value {
    let mut r = Map::new();
    r.insert("typeName".into(), json!(type_name));
    r.insert("fieldName".into(), json!(field));
    r.insert(
        "resolverArn".into(),
        json!(format!(
            "arn:aws:appsync:{}:{}:apis/{api_id}/types/{type_name}/resolvers/{field}",
            ctx.region, ctx.account
        )),
    );
    r.insert(
        "kind".into(),
        json!(body.get("kind").and_then(Value::as_str).unwrap_or("UNIT")),
    );
    copy_keys(
        &mut r,
        body,
        &[
            "dataSourceName",
            "requestMappingTemplate",
            "responseMappingTemplate",
            "pipelineConfig",
            "syncConfig",
            "cachingConfig",
            "maxBatchSize",
            "runtime",
            "code",
            "metricsConfig",
        ],
    );
    Value::Object(r)
}

fn build_function(ctx: &Ctx, api_id: &str, function_id: &str, body: &Value) -> Value {
    let mut f = Map::new();
    f.insert("functionId".into(), json!(function_id));
    f.insert(
        "functionArn".into(),
        json!(format!(
            "arn:aws:appsync:{}:{}:apis/{api_id}/functions/{function_id}",
            ctx.region, ctx.account
        )),
    );
    f.insert(
        "functionVersion".into(),
        json!(body
            .get("functionVersion")
            .and_then(Value::as_str)
            .unwrap_or("2018-05-29")),
    );
    copy_keys(
        &mut f,
        body,
        &[
            "name",
            "description",
            "dataSourceName",
            "requestMappingTemplate",
            "responseMappingTemplate",
            "syncConfig",
            "maxBatchSize",
            "runtime",
            "code",
        ],
    );
    Value::Object(f)
}

fn build_type(ctx: &Ctx, api_id: &str, name: &str, definition: &str, format: &str) -> Value {
    json!({
        "name": name,
        "arn": format!("arn:aws:appsync:{}:{}:apis/{api_id}/types/{name}", ctx.region, ctx.account),
        "definition": definition,
        "format": format,
    })
}

fn build_api_cache(body: &Value) -> Value {
    let mut c = Map::new();
    c.insert("status".into(), json!("AVAILABLE"));
    copy_keys(
        &mut c,
        body,
        &[
            "ttl",
            "apiCachingBehavior",
            "transitEncryptionEnabled",
            "atRestEncryptionEnabled",
            "type",
            "healthMetricsConfig",
        ],
    );
    Value::Object(c)
}

fn build_domain_name(ctx: &Ctx, domain: &str, body: &Value) -> Value {
    let mut c = Map::new();
    c.insert("domainName".into(), json!(domain));
    c.insert(
        "appsyncDomainName".into(),
        json!(format!(
            "{}.appsync-api.{}.amazonaws.com",
            gen_hex(14),
            ctx.region
        )),
    );
    c.insert("hostedZoneId".into(), json!("Z2FDTNDATAQYW2"));
    c.insert(
        "domainNameArn".into(),
        json!(format!(
            "arn:aws:appsync:{}:{}:domainnames/{domain}",
            ctx.region, ctx.account
        )),
    );
    copy_keys(&mut c, body, &["description", "certificateArn", "tags"]);
    Value::Object(c)
}

fn build_event_api(ctx: &Ctx, api_id: &str, body: &Value) -> Value {
    let mut api = Map::new();
    api.insert("apiId".into(), json!(api_id));
    api.insert(
        "apiArn".into(),
        json!(format!(
            "arn:aws:appsync:{}:{}:apis/{api_id}",
            ctx.region, ctx.account
        )),
    );
    api.insert("created".into(), json!(now_epoch()));
    api.insert("xrayEnabled".into(), json!(false));
    api.insert(
        "dns".into(),
        json!({
            "REALTIME": format!("{api_id}.ddpg.appsync-api.{}.amazonaws.com", ctx.region),
            "HTTP": format!("{api_id}.appsync-api.{}.amazonaws.com", ctx.region),
        }),
    );
    copy_keys(
        &mut api,
        body,
        &["name", "ownerContact", "tags", "eventConfig"],
    );
    Value::Object(api)
}

fn build_channel_namespace(ctx: &Ctx, api_id: &str, name: &str, body: &Value) -> Value {
    let mut ns = Map::new();
    ns.insert("apiId".into(), json!(api_id));
    ns.insert("name".into(), json!(name));
    ns.insert(
        "channelNamespaceArn".into(),
        json!(format!(
            "arn:aws:appsync:{}:{}:apis/{api_id}/channelNamespace/{name}",
            ctx.region, ctx.account
        )),
    );
    ns.insert("created".into(), json!(now_epoch()));
    ns.insert("lastModified".into(), json!(now_epoch()));
    copy_keys(
        &mut ns,
        body,
        &[
            "subscribeAuthModes",
            "publishAuthModes",
            "codeHandlers",
            "tags",
            "handlerConfigs",
        ],
    );
    Value::Object(ns)
}

// ===================== helpers =====================

fn ok(v: Value) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, v)
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body).map_err(|e| bad(&format!("Request body is malformed: {e}")))
}

fn bad(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg)
}

/// Reject an empty path label (a too-short probe URI) with `BadRequestException`
/// before resource lookup.
fn require_label(value: &str, field: &str) -> Result<(), AwsServiceError> {
    if value.is_empty() {
        Err(bad(&format!("{field} is required.")))
    } else {
        Ok(())
    }
}

/// Validate an AppSync `resourceArn` path label: it must be a well-formed ARN
/// (`arn:...:appsync:...`) within the declared length bounds. AppSync's
/// `ResourceArn` shape constrains it to `^arn:aws:appsync:...` (length 70..=75);
/// rejecting a non-ARN (or an unsubstituted `{resourceArn}` placeholder) with
/// `BadRequestException` matches the service's own label validation.
fn require_resource_arn(arn: &str) -> Result<(), AwsServiceError> {
    require_label(arn, "resourceArn")?;
    let len = arn.chars().count();
    if !arn.starts_with("arn:") || arn.contains('{') || !(1..=1011).contains(&len) {
        return Err(bad(&format!("resourceArn '{arn}' is not a valid ARN.")));
    }
    Ok(())
}

fn require_body_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad(&format!("{field} is required.")))
}

fn require_query<'a>(q: &'a [(String, String)], key: &str) -> Result<&'a str, AwsServiceError> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad(&format!("{key} query parameter is required.")))
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

/// Copy each present, non-null key from `src` into `dst`.
fn copy_keys(dst: &mut Map<String, Value>, src: &Value, keys: &[&str]) {
    for k in keys {
        if let Some(v) = src.get(*k) {
            if !v.is_null() {
                dst.insert((*k).to_string(), v.clone());
            }
        }
    }
}

fn string_map(obj: &Map<String, Value>) -> std::collections::BTreeMap<String, String> {
    obj.iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect()
}

fn graphql_api_arn(region: &str, account: &str, api_id: &str) -> String {
    format!("arn:aws:appsync:{region}:{account}:apis/{api_id}")
}

/// Whether an API reference (a bare `apiId` or an `...:apis/<id>` ARN) names a
/// GraphQL API that exists in this account. Used by the association ops, which
/// reference their source/merged APIs by identifier.
fn api_ref_exists(data: Option<&AppSyncData>, ident: &str) -> bool {
    let Some(data) = data else {
        return false;
    };
    let id = ident
        .rsplit_once("apis/")
        .map(|(_, tail)| tail.split('/').next().unwrap_or(tail))
        .unwrap_or(ident);
    data.graphql_apis.contains_key(id)
}

fn resolver_key(type_name: &str, field: &str) -> String {
    format!("{type_name}::{field}")
}

/// Extract the top-level type name declared by an SDL `type X { ... }` snippet.
fn parse_type_name(definition: &str) -> Option<String> {
    for line in definition.lines() {
        let t = line.trim();
        for kw in [
            "type ",
            "input ",
            "enum ",
            "interface ",
            "union ",
            "scalar ",
        ] {
            if let Some(rest) = t.strip_prefix(kw) {
                if let Some(name) = rest
                    .split(|c: char| c.is_whitespace() || c == '{' || c == '(' || c == '=')
                    .next()
                {
                    if !name.is_empty() {
                        return Some(name.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Decode a base64 restJson1 blob to a UTF-8 string, tolerating raw (already
/// decoded) input for robustness.
fn decode_blob(b64: &str) -> String {
    match base64::engine::general_purpose::STANDARD.decode(b64) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(_) => b64.to_string(),
    }
}

/// Map internal schema status to the `SchemaStatus` wire enum.
fn schema_status_wire(status: &str) -> &str {
    match status {
        "Processing" => "PROCESSING",
        "Success" => "SUCCESS",
        "Active" => "ACTIVE",
        "Failed" => "FAILED",
        "Deleting" => "DELETING",
        _ => "NOT_APPLICABLE",
    }
}

fn gen_api_id() -> String {
    const CH: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut r = rand::thread_rng();
    (0..26)
        .map(|_| CH[r.gen_range(0..CH.len())] as char)
        .collect()
}

fn gen_hex(n: usize) -> String {
    Uuid::new_v4().simple().to_string()[..n.min(32)].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc() -> AppSyncService {
        AppSyncService::new(Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("000000000000", "us-east-1", ""),
        )))
    }

    fn ctx() -> Ctx {
        Ctx {
            account: "000000000000".to_string(),
            region: "us-east-1".to_string(),
        }
    }

    fn body_json(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).expect("json response body")
    }

    fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        r.err().expect("expected error")
    }

    fn make_api(s: &AppSyncService) -> String {
        let created = body_json(
            &s.create_graphql_api(
                &ctx(),
                &json!({ "name": "demo", "authenticationType": "API_KEY" }),
            )
            .unwrap(),
        );
        created["graphqlApi"]["apiId"].as_str().unwrap().to_string()
    }

    #[test]
    fn create_get_list_delete_graphql_api() {
        let s = svc();
        let api_id = make_api(&s);
        let got = body_json(&s.get_graphql_api(&ctx(), &api_id).unwrap());
        assert_eq!(got["graphqlApi"]["name"], json!("demo"));
        assert!(got["graphqlApi"]["uris"]["GRAPHQL"].is_string());
        let list = body_json(&s.list_graphql_apis(&ctx()).unwrap());
        assert_eq!(list["graphqlApis"].as_array().unwrap().len(), 1);
        s.delete_graphql_api(&ctx(), &api_id).unwrap();
        let err = err_of(s.get_graphql_api(&ctx(), &api_id));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn missing_api_is_not_found() {
        let s = svc();
        let err = err_of(s.get_graphql_api(&ctx(), "nope"));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn api_key_lifecycle() {
        let s = svc();
        let api_id = make_api(&s);
        let key = body_json(&s.create_api_key(&ctx(), &api_id, &json!({})).unwrap());
        let id = key["apiKey"]["id"].as_str().unwrap().to_string();
        assert!(key["apiKey"]["expires"].is_i64());
        let list = body_json(&s.list_api_keys(&ctx(), &api_id).unwrap());
        assert_eq!(list["apiKeys"].as_array().unwrap().len(), 1);
        s.delete_api_key(&ctx(), &api_id, &id).unwrap();
    }

    #[test]
    fn data_source_and_resolver_roundtrip() {
        let s = svc();
        let api_id = make_api(&s);
        let ds = body_json(
            &s.create_data_source(&ctx(), &api_id, &json!({ "name": "src", "type": "NONE" }))
                .unwrap(),
        );
        assert_eq!(ds["dataSource"]["type"], json!("NONE"));
        let r = body_json(
            &s.create_resolver(
                &ctx(),
                &api_id,
                "Query",
                &json!({ "fieldName": "hello", "dataSourceName": "src" }),
            )
            .unwrap(),
        );
        assert_eq!(r["resolver"]["fieldName"], json!("hello"));
        let got = body_json(&s.get_resolver(&ctx(), &api_id, "Query", "hello").unwrap());
        assert_eq!(got["resolver"]["typeName"], json!("Query"));
        let listed = body_json(&s.list_resolvers(&ctx(), &api_id, "Query").unwrap());
        assert_eq!(listed["resolvers"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn resolver_on_missing_api_is_not_found() {
        let s = svc();
        let err =
            err_of(s.create_resolver(&ctx(), "ghost", "Query", &json!({ "fieldName": "hello" })));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn schema_creation_settles_success() {
        let s = svc();
        let api_id = make_api(&s);
        let sdl = base64::engine::general_purpose::STANDARD.encode("type Query { hello: String }");
        s.start_schema_creation(&ctx(), &api_id, &json!({ "definition": sdl }))
            .unwrap();
        let status = body_json(&s.get_schema_creation_status(&ctx(), &api_id).unwrap());
        assert_eq!(status["status"], json!("SUCCESS"));
        let schema = body_json(
            &s.get_introspection_schema(
                &ctx(),
                &api_id,
                &[("format".to_string(), "SDL".to_string())],
            )
            .unwrap(),
        );
        assert!(schema["schema"].is_string());
    }

    #[test]
    fn function_lifecycle() {
        let s = svc();
        let api_id = make_api(&s);
        let f = body_json(
            &s.create_function(
                &ctx(),
                &api_id,
                &json!({ "name": "fn1", "dataSourceName": "src" }),
            )
            .unwrap(),
        );
        let fid = f["functionConfiguration"]["functionId"]
            .as_str()
            .unwrap()
            .to_string();
        let got = body_json(&s.get_function(&ctx(), &api_id, &fid).unwrap());
        assert_eq!(got["functionConfiguration"]["name"], json!("fn1"));
    }

    #[test]
    fn tagging_roundtrip() {
        let s = svc();
        let arn = "arn:aws:appsync:us-east-1:000000000000:apis/abc";
        s.tag_resource(&ctx(), arn, &json!({ "tags": { "team": "obs" } }))
            .unwrap();
        let listed = body_json(&s.list_tags_for_resource(&ctx(), arn).unwrap());
        assert_eq!(listed["tags"]["team"], json!("obs"));
    }

    #[test]
    fn data_source_introspection_start_then_get_is_retrievable() {
        // Regression: StartDataSourceIntrospection persisted nothing and
        // GetDataSourceIntrospection unconditionally 404'd, so the Start -> Get
        // poll flow could never retrieve the discovered schema.
        let s = svc();
        let started = body_json(
            &s.start_ds_introspection(
                &ctx(),
                &json!({
                    "rdsDataApiConfig": {
                        "resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:db",
                        "secretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:s",
                        "databaseName": "app",
                    }
                }),
            )
            .unwrap(),
        );
        let id = started["introspectionId"].as_str().unwrap().to_string();
        assert_eq!(started["introspectionStatus"], json!("SUCCESS"));

        let got = body_json(&s.get_ds_introspection(&ctx(), &id).unwrap());
        assert_eq!(got["introspectionId"], json!(id));
        assert_eq!(got["introspectionStatus"], json!("SUCCESS"));
        assert!(got["introspectionResult"]["models"].is_array());

        // Unknown id is still a declared NotFoundException.
        let err = err_of(s.get_ds_introspection(&ctx(), "does-not-exist"));
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn schema_merge_populates_types_by_association() {
        // Regression: StartSchemaMerge never merged source types and
        // ListTypesByAssociation always returned an empty list.
        let s = svc();
        let source_id = make_api(&s);
        let merged_id = make_api(&s);
        // Define a type on the source API.
        s.create_type(
            &ctx(),
            &source_id,
            &json!({ "definition": "type Widget { id: ID! }", "format": "SDL" }),
        )
        .unwrap();
        // Before a merge there is nothing to list.
        let assoc = body_json(
            &s.associate_source_api(
                &ctx(),
                &merged_id,
                &json!({ "sourceApiIdentifier": source_id }),
            )
            .unwrap(),
        );
        let assoc_id = assoc["sourceApiAssociation"]["associationId"]
            .as_str()
            .unwrap()
            .to_string();
        let fmt = [("format".to_string(), "SDL".to_string())];
        let empty = body_json(
            &s.list_types_by_association(&ctx(), &merged_id, &assoc_id, &fmt)
                .unwrap(),
        );
        assert!(empty["types"].as_array().unwrap().is_empty());

        // After the merge the source type is associated with the merged API.
        let merged = body_json(&s.start_schema_merge(&ctx(), &assoc_id).unwrap());
        assert_eq!(merged["sourceApiAssociationStatus"], json!("MERGE_SUCCESS"));
        let listed = body_json(
            &s.list_types_by_association(&ctx(), &merged_id, &assoc_id, &fmt)
                .unwrap(),
        );
        let types = listed["types"].as_array().unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0]["name"], json!("Widget"));
        // The merged API's own type store now carries the merged type too.
        let on_merged = body_json(&s.list_types(&ctx(), &merged_id, &fmt).unwrap());
        assert_eq!(on_merged["types"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn evaluate_template_renders() {
        let out = body_json(
            &AppSyncService::evaluate_template(&json!({
                "template": "$util.toJson($ctx.arguments)",
                "context": "{\"arguments\":{\"id\":\"1\"}}"
            }))
            .unwrap(),
        );
        assert!(out["evaluationResult"].is_string());
    }

    fn req(method: Method, path: &str) -> AwsRequest {
        AwsRequest {
            service: "appsync".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: path.to_string(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn routing_covers_key_ops() {
        assert_eq!(
            AppSyncService::resolve_action(&req(Method::POST, "/v1/apis"))
                .unwrap()
                .0,
            "CreateGraphqlApi"
        );
        assert_eq!(
            AppSyncService::resolve_action(&req(Method::GET, "/v1/apis/abc"))
                .unwrap()
                .0,
            "GetGraphqlApi"
        );
        assert_eq!(
            AppSyncService::resolve_action(&req(
                Method::GET,
                "/v1/apis/abc/types/Query/resolvers/hello"
            ))
            .unwrap()
            .0,
            "GetResolver"
        );
        assert_eq!(
            AppSyncService::resolve_action(&req(Method::POST, "/v1/apis/abc/ApiCaches/update"))
                .unwrap()
                .0,
            "UpdateApiCache"
        );
        assert_eq!(
            AppSyncService::resolve_action(&req(Method::POST, "/v2/apis"))
                .unwrap()
                .0,
            "CreateApi"
        );
        assert!(AppSyncService::resolve_action(&req(Method::GET, "/v1/nope")).is_none());
    }
}
