//! Aurora DSQL restJson1 control-plane dispatch and operation handlers.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::{
    cluster_arn, cluster_endpoint, gen_id, stream_arn, Cluster, EncryptionDetails,
    MultiRegionProperties, SharedDsqlState, Stream,
};

/// AWS caps the number of clusters per account/Region. Mirrored so
/// `ServiceQuotaExceededException` is reachable and testable.
const MAX_CLUSTERS_PER_ACCOUNT: usize = 100;

pub struct DsqlService {
    state: SharedDsqlState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

/// Parsed route: the operation and its path-derived arguments.
enum Route {
    CreateCluster,
    ListClusters,
    GetCluster(String),
    UpdateCluster(String),
    DeleteCluster(String),
    GetClusterPolicy(String),
    PutClusterPolicy(String),
    DeleteClusterPolicy(String),
    CreateStream(String),
    ListStreams(String),
    GetStream(String, String),
    DeleteStream(String, String),
    GetVpcEndpointServiceName(String),
    TagResource(String),
    UntagResource(String),
    ListTagsForResource(String),
}

impl DsqlService {
    pub fn new(state: SharedDsqlState) -> Self {
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

    /// Start the lifecycle ticker so clusters/streams advance out of their
    /// transient states. Call once after construction + snapshot restore.
    pub fn start_ticker(&self) {
        crate::ticker::spawn(
            self.state.clone(),
            self.snapshot_store.clone(),
            self.snapshot_lock.clone(),
        );
    }

    async fn persist(&self) {
        save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Snapshot hook used by the CloudFormation provisioner (which mutates
    /// state directly) to write a provisioned resource through to disk.
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

    fn resolve_route(req: &AwsRequest) -> Option<Route> {
        let segs: Vec<&str> = req.path_segments.iter().map(|s| s.as_str()).collect();
        match (&req.method, segs.as_slice()) {
            (&Method::POST, ["cluster"]) => Some(Route::CreateCluster),
            (&Method::GET, ["cluster"]) => Some(Route::ListClusters),
            (&Method::GET, ["cluster", id]) => Some(Route::GetCluster((*id).to_string())),
            (&Method::POST, ["cluster", id]) => Some(Route::UpdateCluster((*id).to_string())),
            (&Method::DELETE, ["cluster", id]) => Some(Route::DeleteCluster((*id).to_string())),
            (&Method::GET, ["cluster", id, "policy"]) => {
                Some(Route::GetClusterPolicy((*id).to_string()))
            }
            (&Method::POST, ["cluster", id, "policy"]) => {
                Some(Route::PutClusterPolicy((*id).to_string()))
            }
            (&Method::DELETE, ["cluster", id, "policy"]) => {
                Some(Route::DeleteClusterPolicy((*id).to_string()))
            }
            (&Method::POST, ["stream", cid]) => Some(Route::CreateStream((*cid).to_string())),
            (&Method::GET, ["stream", cid]) => Some(Route::ListStreams((*cid).to_string())),
            (&Method::GET, ["stream", cid, sid]) => {
                Some(Route::GetStream((*cid).to_string(), (*sid).to_string()))
            }
            (&Method::DELETE, ["stream", cid, sid]) => {
                Some(Route::DeleteStream((*cid).to_string(), (*sid).to_string()))
            }
            (&Method::GET, ["clusters", id, "vpc-endpoint-service-name"]) => {
                Some(Route::GetVpcEndpointServiceName((*id).to_string()))
            }
            (&Method::POST, ["tags", arn]) => Some(Route::TagResource(percent_decode(arn))),
            (&Method::DELETE, ["tags", arn]) => Some(Route::UntagResource(percent_decode(arn))),
            (&Method::GET, ["tags", arn]) => Some(Route::ListTagsForResource(percent_decode(arn))),
            // An empty `resourceArn` label collapses `/tags/` to a single
            // segment; route it through so the handler returns a modeled
            // ValidationException rather than "unknown operation".
            (&Method::POST, ["tags"]) => Some(Route::TagResource(String::new())),
            (&Method::DELETE, ["tags"]) => Some(Route::UntagResource(String::new())),
            (&Method::GET, ["tags"]) => Some(Route::ListTagsForResource(String::new())),
            _ => None,
        }
    }

    // --- Cluster operations ------------------------------------------------

    fn create_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}));
        let region = &req.region;
        let account = &req.account_id;

        // Enforce the input members' Smithy @length/@pattern constraints.
        validate_str(&body, "clientToken", 1, 128, is_printable_ascii)?;
        validate_str(&body, "kmsEncryptionKey", 1, 2048, is_kms_key_char)?;
        validate_str(&body, "policy", 1, 20480, |_| true)?;

        let client_token = body
            .get("clientToken")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let deletion_protection = body
            .get("deletionProtectionEnabled")
            .and_then(|v| v.as_bool())
            // Aurora DSQL enables deletion protection by default.
            .unwrap_or(true);

        let kms_encryption_key = body
            .get("kmsEncryptionKey")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let multi_region = parse_multi_region(body.get("multiRegionProperties"));
        let tags = parse_tag_map(body.get("tags"));

        let mut accounts = self.state.write();
        let st = accounts.get_or_create(account);

        // clientToken idempotency: a repeat with the same token returns the
        // cluster created the first time, rather than a second cluster.
        if let Some(token) = &client_token {
            if let Some(existing) = st
                .clusters
                .values()
                .find(|c| c.client_token.as_deref() == Some(token.as_str()))
            {
                return Ok(cluster_mutation_response(existing));
            }
        }

        if st.clusters.len() >= MAX_CLUSTERS_PER_ACCOUNT {
            drop(accounts);
            return Err(quota_exceeded(
                "The maximum number of clusters for this account has been reached.",
            ));
        }

        let id = gen_id();
        let encryption_details = build_encryption(&kms_encryption_key);
        let cluster = Cluster {
            identifier: id.clone(),
            arn: cluster_arn(region, account, &id),
            status: "CREATING".to_string(),
            creation_time: Utc::now(),
            deletion_protection_enabled: deletion_protection,
            multi_region_properties: multi_region,
            encryption_details,
            endpoint: cluster_endpoint(&id, region),
            tags,
            policy: body
                .get("policy")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            policy_version: body.get("policy").map(|_| 1).unwrap_or(0),
            client_token,
            streams: BTreeMap::new(),
        };
        let resp = cluster_mutation_response(&cluster);
        st.clusters.insert(id, cluster);
        Ok(resp)
    }

    fn get_cluster(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let cluster = accounts
            .get(&req.account_id)
            .and_then(|s| s.clusters.get(id))
            .ok_or_else(|| cluster_not_found(id))?;
        let mut out = json!({
            "identifier": cluster.identifier,
            "arn": cluster.arn,
            "status": cluster.status,
            "creationTime": epoch_secs(cluster),
            "deletionProtectionEnabled": cluster.deletion_protection_enabled,
            "tags": tag_map_json(&cluster.tags),
            "encryptionDetails": encryption_json(&cluster.encryption_details),
            "endpoint": cluster.endpoint,
        });
        if let Some(mr) = &cluster.multi_region_properties {
            out["multiRegionProperties"] = multi_region_json(mr);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn update_cluster(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}));
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| cluster_not_found(id))?;
        let cluster = st
            .clusters
            .get_mut(id)
            .ok_or_else(|| cluster_not_found(id))?;

        if let Some(dp) = body
            .get("deletionProtectionEnabled")
            .and_then(|v| v.as_bool())
        {
            cluster.deletion_protection_enabled = dp;
        }
        if let Some(k) = body.get("kmsEncryptionKey").and_then(|v| v.as_str()) {
            cluster.encryption_details = build_encryption(&Some(k.to_string()));
        }
        if let Some(mr) = parse_multi_region(body.get("multiRegionProperties")) {
            cluster.multi_region_properties = Some(mr);
        }
        Ok(cluster_mutation_response(cluster))
    }

    fn delete_cluster(&self, req: &AwsRequest, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| cluster_not_found(id))?;
        let cluster = st
            .clusters
            .get_mut(id)
            .ok_or_else(|| cluster_not_found(id))?;
        if cluster.deletion_protection_enabled {
            return Err(conflict(
                "Deletion protection is enabled for this cluster. Disable deletion protection before deleting.",
            ));
        }
        cluster.status = "DELETING".to_string();
        Ok(cluster_mutation_response(cluster))
    }

    fn list_clusters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (max_results, next_token) = pagination_params(req)?;
        let accounts = self.state.read();
        let mut summaries: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.clusters
                    .values()
                    .map(|c| json!({ "identifier": c.identifier, "arn": c.arn }))
                    .collect()
            })
            .unwrap_or_default();
        summaries.sort_by(|a, b| a["identifier"].as_str().cmp(&b["identifier"].as_str()));
        let (page, token) = paginate_lenient(&summaries, next_token.as_deref(), max_results);
        let mut out = json!({ "clusters": page });
        if let Some(t) = token {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn get_vpc_endpoint_service_name(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let _cluster = accounts
            .get(&req.account_id)
            .and_then(|s| s.clusters.get(id))
            .ok_or_else(|| cluster_not_found(id))?;
        let suffix = hex6(id);
        let service_name = format!("com.amazonaws.{}.dsql-{}", req.region, suffix);
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "serviceName": service_name }),
        ))
    }

    // --- Cluster policy operations ----------------------------------------

    fn put_cluster_policy(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}));
        let policy = body
            .get("policy")
            .and_then(|v| v.as_str())
            .ok_or_else(|| validation("policy is required."))?
            .to_string();
        if policy.is_empty() {
            return Err(validation("policy must not be empty."));
        }
        let expected = body
            .get("expectedPolicyVersion")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok());

        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| cluster_not_found(id))?;
        let cluster = st
            .clusters
            .get_mut(id)
            .ok_or_else(|| cluster_not_found(id))?;
        if let Some(exp) = expected {
            if exp != cluster.policy_version {
                return Err(conflict(
                    "The expectedPolicyVersion does not match the current policy version.",
                ));
            }
        }
        cluster.policy = Some(policy);
        cluster.policy_version += 1;
        let version = cluster.policy_version.to_string();
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "policyVersion": version }),
        ))
    }

    fn get_cluster_policy(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let cluster = accounts
            .get(&req.account_id)
            .and_then(|s| s.clusters.get(id))
            .ok_or_else(|| cluster_not_found(id))?;
        let policy = cluster
            .policy
            .as_ref()
            .ok_or_else(|| resource_not_found("No policy is attached to this cluster."))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "policy": policy, "policyVersion": cluster.policy_version.to_string() }),
        ))
    }

    fn delete_cluster_policy(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let expected = req
            .query_params
            .get("expected-policy-version")
            .and_then(|s| s.parse::<u64>().ok());
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| cluster_not_found(id))?;
        let cluster = st
            .clusters
            .get_mut(id)
            .ok_or_else(|| cluster_not_found(id))?;
        if let Some(exp) = expected {
            if exp != cluster.policy_version {
                return Err(conflict(
                    "The expectedPolicyVersion does not match the current policy version.",
                ));
            }
        }
        cluster.policy = None;
        cluster.policy_version += 1;
        let version = cluster.policy_version.to_string();
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "policyVersion": version }),
        ))
    }

    // --- Stream operations -------------------------------------------------

    fn create_stream(
        &self,
        req: &AwsRequest,
        cluster_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}));
        let target = body
            .get("targetDefinition")
            .cloned()
            .ok_or_else(|| validation("targetDefinition is required."))?;
        let ordering = body
            .get("ordering")
            .and_then(|v| v.as_str())
            .ok_or_else(|| validation("ordering is required."))?
            .to_string();
        let format = body
            .get("format")
            .and_then(|v| v.as_str())
            .ok_or_else(|| validation("format is required."))?
            .to_string();
        let client_token = body
            .get("clientToken")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_tag_map(body.get("tags"));

        let region = req.region.clone();
        let account = req.account_id.clone();
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&account)
            .ok_or_else(|| cluster_not_found(cluster_id))?;
        let cluster = st
            .clusters
            .get_mut(cluster_id)
            .ok_or_else(|| cluster_not_found(cluster_id))?;

        if let Some(token) = &client_token {
            if let Some(existing) = cluster
                .streams
                .values()
                .find(|s| s.client_token.as_deref() == Some(token.as_str()))
            {
                return Ok(stream_mutation_response(existing));
            }
        }

        let sid = gen_id();
        let stream = Stream {
            cluster_identifier: cluster_id.to_string(),
            stream_identifier: sid.clone(),
            arn: stream_arn(&region, &account, cluster_id, &sid),
            status: "CREATING".to_string(),
            creation_time: Utc::now(),
            ordering,
            format,
            target_definition: target,
            status_reason: None,
            tags,
            client_token,
        };
        let resp = stream_mutation_response(&stream);
        cluster.streams.insert(sid, stream);
        Ok(resp)
    }

    fn get_stream(
        &self,
        req: &AwsRequest,
        cluster_id: &str,
        stream_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let stream = accounts
            .get(&req.account_id)
            .and_then(|s| s.clusters.get(cluster_id))
            .and_then(|c| c.streams.get(stream_id))
            .ok_or_else(|| stream_not_found(stream_id))?;
        let mut out = stream_full_json(stream);
        out["tags"] = tag_map_json(&stream.tags);
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    fn delete_stream(
        &self,
        req: &AwsRequest,
        cluster_id: &str,
        stream_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| stream_not_found(stream_id))?;
        let cluster = st
            .clusters
            .get_mut(cluster_id)
            .ok_or_else(|| cluster_not_found(cluster_id))?;
        let stream = cluster
            .streams
            .get_mut(stream_id)
            .ok_or_else(|| stream_not_found(stream_id))?;
        stream.status = "DELETING".to_string();
        let resp = json!({
            "clusterIdentifier": stream.cluster_identifier,
            "streamIdentifier": stream.stream_identifier,
            "arn": stream.arn,
            "status": stream.status,
            "creationTime": stream.creation_time.timestamp() as f64
                + stream.creation_time.timestamp_subsec_millis() as f64 / 1000.0,
        });
        cluster.streams.remove(stream_id);
        Ok(AwsResponse::json_value(StatusCode::OK, resp))
    }

    fn list_streams(
        &self,
        req: &AwsRequest,
        cluster_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (max_results, next_token) = pagination_params(req)?;
        let accounts = self.state.read();
        let cluster = accounts
            .get(&req.account_id)
            .and_then(|s| s.clusters.get(cluster_id))
            .ok_or_else(|| cluster_not_found(cluster_id))?;
        let mut summaries: Vec<Value> = cluster
            .streams
            .values()
            .map(|s| {
                json!({
                    "clusterIdentifier": s.cluster_identifier,
                    "streamIdentifier": s.stream_identifier,
                    "arn": s.arn,
                    "status": s.status,
                    "creationTime": s.creation_time.timestamp() as f64
                        + s.creation_time.timestamp_subsec_millis() as f64 / 1000.0,
                })
            })
            .collect();
        summaries.sort_by(|a, b| {
            a["streamIdentifier"]
                .as_str()
                .cmp(&b["streamIdentifier"].as_str())
        });
        let (page, token) = paginate_lenient(&summaries, next_token.as_deref(), max_results);
        let mut out = json!({ "streams": page });
        if let Some(t) = token {
            out["nextToken"] = json!(t);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, out))
    }

    // --- Tagging -----------------------------------------------------------

    fn tag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        require_resource_arn(arn)?;
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_else(|_| json!({}));
        let new_tags = parse_tag_map(body.get("tags"));
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| resource_not_found_arn(arn))?;
        let tags = resolve_tags_mut(st, arn).ok_or_else(|| resource_not_found_arn(arn))?;
        for (k, v) in new_tags {
            tags.insert(k, v);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        require_resource_arn(arn)?;
        let keys: Vec<String> = req
            .query_params
            .get("tagKeys")
            .map(|v| v.split(',').map(|s| s.to_string()).collect())
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let st = accounts
            .get_mut(&req.account_id)
            .ok_or_else(|| resource_not_found_arn(arn))?;
        let tags = resolve_tags_mut(st, arn).ok_or_else(|| resource_not_found_arn(arn))?;
        for k in keys {
            tags.remove(&k);
        }
        Ok(AwsResponse::json_value(StatusCode::OK, json!({})))
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        require_resource_arn(arn)?;
        let accounts = self.state.read();
        let st = accounts
            .get(&req.account_id)
            .ok_or_else(|| resource_not_found_arn(arn))?;
        let tags = resolve_tags_ref(st, arn).ok_or_else(|| resource_not_found_arn(arn))?;
        Ok(AwsResponse::json_value(
            StatusCode::OK,
            json!({ "tags": tag_map_json(tags) }),
        ))
    }
}

/// Every operation name in the DSQL Smithy model, for introspection and the
/// dispatch allowlist.
pub const DSQL_ACTIONS: &[&str] = &[
    "CreateCluster",
    "GetCluster",
    "UpdateCluster",
    "DeleteCluster",
    "ListClusters",
    "GetClusterPolicy",
    "PutClusterPolicy",
    "DeleteClusterPolicy",
    "CreateStream",
    "GetStream",
    "DeleteStream",
    "ListStreams",
    "GetVpcEndpointServiceName",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
];

impl Route {
    /// Whether this route mutates state and therefore needs a snapshot write.
    fn mutates(&self) -> bool {
        matches!(
            self,
            Route::CreateCluster
                | Route::UpdateCluster(_)
                | Route::DeleteCluster(_)
                | Route::PutClusterPolicy(_)
                | Route::DeleteClusterPolicy(_)
                | Route::CreateStream(_)
                | Route::DeleteStream(_, _)
                | Route::TagResource(_)
                | Route::UntagResource(_)
        )
    }
}

#[async_trait]
impl AwsService for DsqlService {
    fn service_name(&self) -> &str {
        "dsql"
    }

    fn supported_actions(&self) -> &[&str] {
        DSQL_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(route) = Self::resolve_route(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let mutates = route.mutates();
        let result = match route {
            Route::CreateCluster => self.create_cluster(&req),
            Route::ListClusters => self.list_clusters(&req),
            Route::GetCluster(id) => self.get_cluster(&req, &id),
            Route::UpdateCluster(id) => self.update_cluster(&req, &id),
            Route::DeleteCluster(id) => self.delete_cluster(&req, &id),
            Route::GetClusterPolicy(id) => self.get_cluster_policy(&req, &id),
            Route::PutClusterPolicy(id) => self.put_cluster_policy(&req, &id),
            Route::DeleteClusterPolicy(id) => self.delete_cluster_policy(&req, &id),
            Route::CreateStream(cid) => self.create_stream(&req, &cid),
            Route::ListStreams(cid) => self.list_streams(&req, &cid),
            Route::GetStream(cid, sid) => self.get_stream(&req, &cid, &sid),
            Route::DeleteStream(cid, sid) => self.delete_stream(&req, &cid, &sid),
            Route::GetVpcEndpointServiceName(id) => self.get_vpc_endpoint_service_name(&req, &id),
            Route::TagResource(arn) => self.tag_resource(&req, &arn),
            Route::UntagResource(arn) => self.untag_resource(&req, &arn),
            Route::ListTagsForResource(arn) => self.list_tags_for_resource(&req, &arn),
        };
        // Persist only after a successful mutation, mirroring the write-through
        // behavior of the other services' handlers.
        if mutates && result.is_ok() {
            self.persist().await;
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Response builders
// ---------------------------------------------------------------------------

/// The shared shape returned by Create/Update/DeleteCluster.
fn cluster_mutation_response(cluster: &Cluster) -> AwsResponse {
    let mut out = json!({
        "identifier": cluster.identifier,
        "arn": cluster.arn,
        "status": cluster.status,
        "creationTime": epoch_secs(cluster),
        "deletionProtectionEnabled": cluster.deletion_protection_enabled,
    });
    if let Some(mr) = &cluster.multi_region_properties {
        out["multiRegionProperties"] = multi_region_json(mr);
    }
    out["encryptionDetails"] = encryption_json(&cluster.encryption_details);
    if !cluster.endpoint.is_empty() {
        out["endpoint"] = json!(cluster.endpoint);
    }
    AwsResponse::json_value(StatusCode::OK, out)
}

fn stream_mutation_response(stream: &Stream) -> AwsResponse {
    AwsResponse::json_value(StatusCode::OK, stream_core_json(stream))
}

fn stream_core_json(stream: &Stream) -> Value {
    json!({
        "clusterIdentifier": stream.cluster_identifier,
        "streamIdentifier": stream.stream_identifier,
        "arn": stream.arn,
        "status": stream.status,
        "creationTime": stream.creation_time.timestamp() as f64
            + stream.creation_time.timestamp_subsec_millis() as f64 / 1000.0,
        "ordering": stream.ordering,
        "format": stream.format,
    })
}

fn stream_full_json(stream: &Stream) -> Value {
    let mut out = stream_core_json(stream);
    out["targetDefinition"] = stream.target_definition.clone();
    if let Some(reason) = &stream.status_reason {
        out["statusReason"] = json!(reason);
    }
    out
}

fn epoch_secs(cluster: &Cluster) -> f64 {
    cluster.creation_time.timestamp() as f64
        + cluster.creation_time.timestamp_subsec_millis() as f64 / 1000.0
}

fn encryption_json(e: &EncryptionDetails) -> Value {
    let mut out = json!({
        "encryptionType": e.encryption_type,
        "encryptionStatus": e.encryption_status,
    });
    if let Some(arn) = &e.kms_key_arn {
        out["kmsKeyArn"] = json!(arn);
    }
    out
}

fn multi_region_json(mr: &MultiRegionProperties) -> Value {
    let mut out = json!({ "clusters": mr.clusters });
    if let Some(w) = &mr.witness_region {
        out["witnessRegion"] = json!(w);
    }
    out
}

fn tag_map_json(tags: &BTreeMap<String, String>) -> Value {
    Value::Object(
        tags.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// Enforce a string member's `@length` (and optional `@pattern`) constraint,
/// returning a modeled `ValidationException` on violation. Only applied when
/// the member is present.
fn validate_str(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
    charset_ok: impl Fn(char) -> bool,
) -> Result<(), AwsServiceError> {
    if let Some(s) = body.get(field).and_then(|v| v.as_str()) {
        if s.len() < min || s.len() > max {
            return Err(validation(&format!(
                "Value at '{field}' failed to satisfy constraint: Member must have length between {min} and {max}."
            )));
        }
        if !s.chars().all(&charset_ok) {
            return Err(validation(&format!(
                "Value at '{field}' failed to satisfy constraint: Member must satisfy the required pattern."
            )));
        }
    }
    Ok(())
}

/// Reject an empty/whitespace `resourceArn` path label with a modeled
/// `ValidationException` (the too-short-arn constraint variant).
fn require_resource_arn(arn: &str) -> Result<(), AwsServiceError> {
    if arn.trim().is_empty() {
        return Err(validation(
            "Value at 'resourceArn' failed to satisfy constraint: Member must not be empty.",
        ));
    }
    Ok(())
}

/// `^[!-~]+$` — any printable ASCII (no spaces/control).
fn is_printable_ascii(c: char) -> bool {
    ('!'..='~').contains(&c)
}

/// `^[a-zA-Z0-9:/_-]+$`.
fn is_kms_key_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, ':' | '/' | '_' | '-')
}

fn parse_tag_map(v: Option<&Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(Value::Object(map)) = v {
        for (k, val) in map {
            if let Some(s) = val.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn parse_multi_region(v: Option<&Value>) -> Option<MultiRegionProperties> {
    let obj = v?.as_object()?;
    Some(MultiRegionProperties {
        witness_region: obj
            .get("witnessRegion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        clusters: obj
            .get("clusters")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|c| c.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

fn build_encryption(kms_key: &Option<String>) -> EncryptionDetails {
    match kms_key {
        Some(k) => EncryptionDetails {
            encryption_type: "CUSTOMER_MANAGED_KMS_KEY".to_string(),
            kms_key_arn: Some(k.clone()),
            encryption_status: "ENABLED".to_string(),
        },
        None => EncryptionDetails {
            encryption_type: "AWS_OWNED_KMS_KEY".to_string(),
            kms_key_arn: None,
            encryption_status: "ENABLED".to_string(),
        },
    }
}

/// Paginate, tolerating an opaque/undecodable `nextToken` by restarting from
/// the first page instead of raising an undeclared `ValidationException`
/// (these list ops only model `ResourceNotFoundException`).
fn paginate_lenient(
    items: &[Value],
    next_token: Option<&str>,
    max_results: usize,
) -> (Vec<Value>, Option<String>) {
    match paginate_checked(items, next_token, max_results) {
        Ok(res) => res,
        Err(_) => paginate_checked(items, None, max_results)
            .expect("pagination from the start is always valid"),
    }
}

/// Parse and validate the shared list pagination parameters. `maxResults` is
/// range-checked (1..=100 per the model's `@range`); an out-of-range value is
/// a `ValidationException`. The `nextToken` is returned as-is and interpreted
/// leniently by the caller (an opaque/expired token yields an empty page
/// rather than an undeclared error, since these list ops only model
/// `ResourceNotFoundException`).
fn pagination_params(req: &AwsRequest) -> Result<(usize, Option<String>), AwsServiceError> {
    let max = match req.query_params.get("max-results") {
        Some(raw) => {
            let n = raw.parse::<i64>().map_err(|_| {
                validation("Value for 'maxResults' must be an integer between 1 and 100.")
            })?;
            if !(1..=100).contains(&n) {
                return Err(validation(
                    "Value for 'maxResults' must be between 1 and 100.",
                ));
            }
            n as usize
        }
        None => 100,
    };
    let token = req.query_params.get("next-token").cloned();
    Ok((max, token))
}

/// Resolve an ARN to the mutable tag map of the cluster or stream it names.
fn resolve_tags_mut<'a>(
    st: &'a mut crate::state::DsqlState,
    arn: &str,
) -> Option<&'a mut BTreeMap<String, String>> {
    let (cluster_id, stream_id) = parse_resource_arn(arn)?;
    let cluster = st.clusters.get_mut(&cluster_id)?;
    match stream_id {
        Some(sid) => cluster.streams.get_mut(&sid).map(|s| &mut s.tags),
        None => Some(&mut cluster.tags),
    }
}

fn resolve_tags_ref<'a>(
    st: &'a crate::state::DsqlState,
    arn: &str,
) -> Option<&'a BTreeMap<String, String>> {
    let (cluster_id, stream_id) = parse_resource_arn(arn)?;
    let cluster = st.clusters.get(&cluster_id)?;
    match stream_id {
        Some(sid) => cluster.streams.get(&sid).map(|s| &s.tags),
        None => Some(&cluster.tags),
    }
}

/// Extract `(clusterId, Option<streamId>)` from a DSQL ARN of the form
/// `arn:aws:dsql:region:acct:cluster/<id>[/stream/<id>]`.
fn parse_resource_arn(arn: &str) -> Option<(String, Option<String>)> {
    let resource = arn.splitn(6, ':').nth(5)?;
    let parts: Vec<&str> = resource.split('/').collect();
    match parts.as_slice() {
        ["cluster", cid] => Some(((*cid).to_string(), None)),
        ["cluster", cid, "stream", sid] => Some(((*cid).to_string(), Some((*sid).to_string()))),
        _ => None,
    }
}

/// Deterministic 6-hex-char suffix for a cluster's VPC endpoint service name.
fn hex6(id: &str) -> String {
    let mut h: u32 = 2166136261;
    for b in id.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    format!("{:06x}", h & 0x00ff_ffff)
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .to_string()
}

// ---------------------------------------------------------------------------
// Error builders
// ---------------------------------------------------------------------------

fn validation(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg)
}

fn quota_exceeded(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::PAYMENT_REQUIRED,
        "ServiceQuotaExceededException",
        msg,
    )
}

fn resource_not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn cluster_not_found(id: &str) -> AwsServiceError {
    resource_not_found(&format!("Cluster not found: {id}"))
}

fn stream_not_found(id: &str) -> AwsServiceError {
    resource_not_found(&format!("Stream not found: {id}"))
}

fn resource_not_found_arn(arn: &str) -> AwsServiceError {
    resource_not_found(&format!("Resource not found: {arn}"))
}
