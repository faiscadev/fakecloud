//! AWS EKS REST-JSON service handler.
//!
//! Implements the cluster control plane (Create/Describe/List/DeleteCluster,
//! UpdateClusterConfig, UpdateClusterVersion, Describe/ListUpdates,
//! Tag/Untag/ListTagsForResource) plus managed node groups
//! (Create/Describe/List/DeleteNodegroup, UpdateNodegroupConfig/Version) and
//! Fargate profiles (Create/Describe/List/DeleteFargateProfile), both modeled
//! as cluster sub-resources. Addons, access entries, and identity-provider
//! configs land in later batches.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    addon_arn, cluster_arn, fargate_profile_arn, nodegroup_arn, pod_identity_association_arn,
    Addon, Cluster, EksSnapshot, FargateProfile, Nodegroup, SharedEksState, Update,
    DEFAULT_K8S_VERSION, EKS_SNAPSHOT_SCHEMA_VERSION,
};

/// The set of control-plane log types EKS reports on every cluster.
const LOG_TYPES: &[&str] = &[
    "api",
    "audit",
    "authenticator",
    "controllerManager",
    "scheduler",
];

pub const EKS_ACTIONS: &[&str] = &[
    "CreateCluster",
    "DescribeCluster",
    "ListClusters",
    "DeleteCluster",
    "UpdateClusterConfig",
    "UpdateClusterVersion",
    "DescribeUpdate",
    "ListUpdates",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "CreateNodegroup",
    "DescribeNodegroup",
    "ListNodegroups",
    "DeleteNodegroup",
    "UpdateNodegroupConfig",
    "UpdateNodegroupVersion",
    "CreateFargateProfile",
    "DescribeFargateProfile",
    "ListFargateProfiles",
    "DeleteFargateProfile",
    "CreateAddon",
    "DescribeAddon",
    "ListAddons",
    "DeleteAddon",
    "UpdateAddon",
    "DescribeAddonVersions",
    "DescribeAddonConfiguration",
];

pub struct EksService {
    state: SharedEksState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

enum PathArgs {
    None,
    Name(String),
    Update {
        name: String,
        update_id: String,
    },
    Arn(String),
    /// A sub-resource collection scoped to a cluster (create/list).
    Cluster(String),
    /// A named sub-resource of a cluster (describe/delete/update).
    ClusterChild {
        cluster: String,
        name: String,
    },
}

impl EksService {
    pub fn new(state: SharedEksState) -> Self {
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
        save_eks_snapshot(
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
                save_eks_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, PathArgs)> {
        // Derive segments from the raw path, preserving INTERNAL empty segments
        // (only the leading empty from the leading `/` and a single trailing
        // empty are dropped). `req.path_segments` strips every empty part, which
        // would collapse a real 3-segment path with an empty middle label
        // (e.g. `/clusters//addons`, ListAddons with an empty clusterName) down
        // to `["clusters","addons"]` and mis-route it to DescribeCluster. Keeping
        // internal empties makes the exact-slice matches below precise, so a
        // not-yet-implemented sub-resource path falls through to None.
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let trimmed = trimmed.strip_suffix('/').unwrap_or(trimmed);
        let segs: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        match (&req.method, segs.as_slice()) {
            (&Method::POST, ["clusters"]) => Some(("CreateCluster", PathArgs::None)),
            (&Method::GET, ["clusters"]) => Some(("ListClusters", PathArgs::None)),
            (&Method::GET, ["clusters", name]) => {
                Some(("DescribeCluster", PathArgs::Name(decode(name))))
            }
            (&Method::DELETE, ["clusters", name]) => {
                Some(("DeleteCluster", PathArgs::Name(decode(name))))
            }
            (&Method::POST, ["clusters", name, "update-config"]) => {
                Some(("UpdateClusterConfig", PathArgs::Name(decode(name))))
            }
            (&Method::POST, ["clusters", name, "updates"]) => {
                Some(("UpdateClusterVersion", PathArgs::Name(decode(name))))
            }
            (&Method::GET, ["clusters", name, "updates"]) => {
                Some(("ListUpdates", PathArgs::Name(decode(name))))
            }
            (&Method::GET, ["clusters", name, "updates", update_id]) => Some((
                "DescribeUpdate",
                PathArgs::Update {
                    name: decode(name),
                    update_id: decode(update_id),
                },
            )),
            // Node groups (sub-resources of a cluster).
            (&Method::POST, ["clusters", c, "node-groups"]) => {
                Some(("CreateNodegroup", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "node-groups"]) => {
                Some(("ListNodegroups", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "node-groups", n]) => Some((
                "DescribeNodegroup",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::DELETE, ["clusters", c, "node-groups", n]) => Some((
                "DeleteNodegroup",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::POST, ["clusters", c, "node-groups", n, "update-config"]) => Some((
                "UpdateNodegroupConfig",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::POST, ["clusters", c, "node-groups", n, "update-version"]) => Some((
                "UpdateNodegroupVersion",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            // Fargate profiles (sub-resources of a cluster).
            (&Method::POST, ["clusters", c, "fargate-profiles"]) => {
                Some(("CreateFargateProfile", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "fargate-profiles"]) => {
                Some(("ListFargateProfiles", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "fargate-profiles", n]) => Some((
                "DescribeFargateProfile",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::DELETE, ["clusters", c, "fargate-profiles", n]) => Some((
                "DeleteFargateProfile",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            // Add-ons (sub-resources of a cluster).
            (&Method::POST, ["clusters", c, "addons"]) => {
                Some(("CreateAddon", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "addons"]) => {
                Some(("ListAddons", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "addons", n]) => Some((
                "DescribeAddon",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::DELETE, ["clusters", c, "addons", n]) => Some((
                "DeleteAddon",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::POST, ["clusters", c, "addons", n, "update"]) => Some((
                "UpdateAddon",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            // Add-on catalogue ops (not scoped to a cluster).
            (&Method::GET, ["addons", "supported-versions"]) => {
                Some(("DescribeAddonVersions", PathArgs::None))
            }
            (&Method::GET, ["addons", "configuration-schemas"]) => {
                Some(("DescribeAddonConfiguration", PathArgs::None))
            }
            (&Method::POST, ["tags", arn]) => Some(("TagResource", PathArgs::Arn(decode(arn)))),
            (&Method::DELETE, ["tags", arn]) => Some(("UntagResource", PathArgs::Arn(decode(arn)))),
            (&Method::GET, ["tags", arn]) => {
                Some(("ListTagsForResource", PathArgs::Arn(decode(arn))))
            }
            _ => None,
        }
    }

    fn create_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();

        let name = body
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("name is required"))?
            .to_string();
        validate_cluster_name(&name)?;

        let role_arn = body
            .get("roleArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("roleArn is required"))?
            .to_string();

        let vpc_req = body
            .get("resourcesVpcConfig")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("resourcesVpcConfig is required"))?;

        let version = body
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_K8S_VERSION)
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.clusters.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!("Cluster already exists with name: {name}"),
            ));
        }

        let region = req.region.clone();
        let account_id = req.account_id.clone();
        let arn = cluster_arn(&region, &account_id, &name);
        let id = uuid::Uuid::new_v4().to_string();

        let tags = parse_tag_map(body.get("tags"));

        let cluster = Cluster {
            name: name.clone(),
            arn: arn.clone(),
            version,
            role_arn,
            status: "CREATING".to_string(),
            created_at: Utc::now(),
            endpoint: format!(
                "https://{}.gr7.{region}.eks.amazonaws.com",
                id.replace('-', "").to_uppercase()
            ),
            platform_version: "eks.1".to_string(),
            certificate_authority_data: default_ca_data(),
            resources_vpc_config: build_vpc_config_response(vpc_req, &id),
            kubernetes_network_config: build_k8s_network_config(
                body.get("kubernetesNetworkConfig"),
            ),
            logging: build_logging(body.get("logging")),
            tags,
            updates: Default::default(),
        };

        let out = cluster_json(&cluster, &id);
        state.clusters.insert(name, cluster);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": out }).to_string(),
        ))
    }

    fn describe_cluster(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;
        // Lazily settle CREATING -> ACTIVE on first describe, mirroring the
        // eventual-consistency of the real control plane.
        if cluster.status == "CREATING" {
            cluster.status = "ACTIVE".to_string();
        }
        let id = arn_cluster_id(&cluster.endpoint);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": cluster_json(cluster, &id) }).to_string(),
        ))
    }

    fn list_clusters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();

        let accounts = self.state.read();
        let Some(state) = accounts.get(&req.account_id) else {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                json!({ "clusters": [] }).to_string(),
            ));
        };
        let names: Vec<String> = state.clusters.keys().cloned().collect();
        let (page, token) = paginate_checked(&names, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "clusters": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_cluster(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut cluster = state
            .clusters
            .remove(name)
            .ok_or_else(not_found_cluster(name))?;
        cluster.status = "DELETING".to_string();
        let id = arn_cluster_id(&cluster.endpoint);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": cluster_json(&cluster, &id) }).to_string(),
        ))
    }

    fn update_cluster_config(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;

        let (update_type, params) = if let Some(logging) = body.get("logging") {
            cluster.logging = build_logging(Some(logging));
            (
                "LoggingUpdate",
                vec![("ClusterLogging".to_string(), logging.to_string())],
            )
        } else if let Some(vpc) = body.get("resourcesVpcConfig") {
            let id = arn_cluster_id(&cluster.endpoint);
            cluster.resources_vpc_config = build_vpc_config_response(vpc, &id);
            (
                "EndpointAccessUpdate",
                vec![("EndpointPublicAccess".to_string(), vpc.to_string())],
            )
        } else {
            ("ConfigUpdate", Vec::new())
        };

        let update = new_update(update_type, params);
        let out = update_json(&update);
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    fn update_cluster_version(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let version = body
            .get("version")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("version is required"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;
        cluster.version = version.clone();

        let update = new_update(
            "VersionUpdate",
            vec![
                ("Version".to_string(), version),
                (
                    "PlatformVersion".to_string(),
                    cluster.platform_version.clone(),
                ),
            ],
        );
        let out = update_json(&update);
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    fn describe_update(
        &self,
        req: &AwsRequest,
        name: &str,
        update_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let nodegroup_name = req.query_params.get("nodegroupName").cloned();
        let addon_name = req.query_params.get("addonName").cloned();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(name) {
            return Err(not_found_cluster(name)());
        }
        // `DescribeUpdate` disambiguates cluster updates from node-group and
        // add-on updates via the optional `nodegroupName` / `addonName` query
        // params, mirroring the real API. When present, the update lives on
        // that sub-resource's own update history.
        let update = if let Some(ng_name) = nodegroup_name.as_deref() {
            let ng = state
                .nodegroups
                .get_mut(name)
                .and_then(|m| m.get_mut(ng_name))
                .ok_or_else(not_found_nodegroup(ng_name))?;
            ng.updates
                .get_mut(update_id)
                .ok_or_else(not_found_update(update_id))?
        } else if let Some(a_name) = addon_name.as_deref() {
            let addon = state
                .addons
                .get_mut(name)
                .and_then(|m| m.get_mut(a_name))
                .ok_or_else(not_found_addon(a_name))?;
            addon
                .updates
                .get_mut(update_id)
                .ok_or_else(not_found_update(update_id))?
        } else {
            let cluster = state
                .clusters
                .get_mut(name)
                .ok_or_else(not_found_cluster(name))?;
            cluster
                .updates
                .get_mut(update_id)
                .ok_or_else(not_found_update(update_id))?
        };
        // Updates settle to Successful on first describe.
        if update.status == "InProgress" {
            update.status = "Successful".to_string();
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": update_json(update) }).to_string(),
        ))
    }

    fn list_updates(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();

        let nodegroup_name = req.query_params.get("nodegroupName").cloned();
        let addon_name = req.query_params.get("addonName").cloned();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(name))?;
        if !state.clusters.contains_key(name) {
            return Err(not_found_cluster(name)());
        }
        let ids: Vec<String> = if let Some(ng_name) = nodegroup_name.as_deref() {
            let ng = state
                .nodegroups
                .get(name)
                .and_then(|m| m.get(ng_name))
                .ok_or_else(not_found_nodegroup(ng_name))?;
            ng.updates.keys().cloned().collect()
        } else if let Some(a_name) = addon_name.as_deref() {
            let addon = state
                .addons
                .get(name)
                .and_then(|m| m.get(a_name))
                .ok_or_else(not_found_addon(a_name))?;
            addon.updates.keys().cloned().collect()
        } else {
            let cluster = state
                .clusters
                .get(name)
                .ok_or_else(not_found_cluster(name))?;
            cluster.updates.keys().cloned().collect()
        };
        let (page, token) = paginate_checked(&ids, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "updateIds": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn tag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let tags = body
            .get("tags")
            .filter(|v| v.is_object())
            .ok_or_else(|| bad_request("tags is required"))?;
        let name = cluster_name_from_arn(arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(not_found_arn(arn))?;
        for (k, v) in tags.as_object().unwrap() {
            if let Some(v) = v.as_str() {
                cluster.tags.insert(k.clone(), v.to_string());
            }
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let keys = parse_multi_query(&req.raw_query, "tagKeys");
        let name = cluster_name_from_arn(arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(not_found_arn(arn))?;
        for k in keys {
            cluster.tags.remove(&k);
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = cluster_name_from_arn(arn)?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_arn(arn))?;
        let cluster = state.clusters.get(&name).ok_or_else(not_found_arn(arn))?;
        let tags: serde_json::Map<String, Value> = cluster
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "tags": tags }).to_string(),
        ))
    }

    // -----------------------------------------------------------------------
    // Node groups
    // -----------------------------------------------------------------------

    fn create_nodegroup(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("nodegroupName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("nodegroupName is required"))?
            .to_string();
        let node_role = body
            .get("nodeRole")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("nodeRole is required"))?
            .to_string();
        let subnets = body
            .get("subnets")
            .filter(|v| v.is_array())
            .cloned()
            .ok_or_else(|| invalid_parameter("subnets is required"))?;

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // The parent cluster must exist. AWS returns ResourceNotFoundException
        // here even though the Smithy model omits it from CreateNodegroup's
        // declared errors.
        let cluster_version = state
            .clusters
            .get(cluster_name)
            .ok_or_else(not_found_cluster(cluster_name))?
            .version
            .clone();
        if state
            .nodegroups
            .get(cluster_name)
            .is_some_and(|m| m.contains_key(&name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!(
                    "NodeGroup already exists with name {name} and cluster name {cluster_name}"
                ),
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let arn = nodegroup_arn(&region, &account_id, cluster_name, &name, &id);
        let now = Utc::now();
        let version = body
            .get("version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(cluster_version);
        let release_version = body
            .get("releaseVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{version}-20240000"));
        let capacity_type = body
            .get("capacityType")
            .and_then(|v| v.as_str())
            .unwrap_or("ON_DEMAND")
            .to_string();
        let ami_type = body
            .get("amiType")
            .and_then(|v| v.as_str())
            .unwrap_or("AL2023_x86_64_STANDARD")
            .to_string();
        let disk_size = body.get("diskSize").and_then(|v| v.as_i64()).unwrap_or(20);

        let ng = Nodegroup {
            name: name.clone(),
            arn,
            cluster_name: cluster_name.to_string(),
            version,
            release_version,
            status: "CREATING".to_string(),
            capacity_type,
            ami_type,
            node_role,
            created_at: now,
            modified_at: now,
            disk_size,
            scaling_config: build_scaling_config(body.get("scalingConfig")),
            update_config: build_nodegroup_update_config(body.get("updateConfig")),
            instance_types: body
                .get("instanceTypes")
                .cloned()
                .unwrap_or_else(|| json!(["t3.medium"])),
            subnets,
            labels: body.get("labels").cloned().unwrap_or_else(|| json!({})),
            taints: body.get("taints").cloned().unwrap_or_else(|| json!([])),
            remote_access: body.get("remoteAccess").cloned(),
            launch_template: body.get("launchTemplate").cloned(),
            asg_name: format!("eks-{name}-{}", &id[..8]),
            tags: parse_tag_map(body.get("tags")),
            updates: Default::default(),
        };

        let out = nodegroup_json(&ng);
        state
            .nodegroups
            .entry(cluster_name.to_string())
            .or_default()
            .insert(name, ng);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "nodegroup": out }).to_string(),
        ))
    }

    fn describe_nodegroup(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let ng = state
            .nodegroups
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_nodegroup(name))?;
        if ng.status == "CREATING" {
            ng.status = "ACTIVE".to_string();
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "nodegroup": nodegroup_json(ng) }).to_string(),
        ))
    }

    fn list_nodegroups(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(cluster_name))?;
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let names: Vec<String> = state
            .nodegroups
            .get(cluster_name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let (page, token) = paginate_checked(&names, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "nodegroups": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_nodegroup(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let mut ng = state
            .nodegroups
            .get_mut(cluster_name)
            .and_then(|m| m.remove(name))
            .ok_or_else(not_found_nodegroup(name))?;
        ng.status = "DELETING".to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "nodegroup": nodegroup_json(&ng) }).to_string(),
        ))
    }

    fn update_nodegroup_config(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let ng = state
            .nodegroups
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_nodegroup(name))?;

        let mut params = Vec::new();
        if let Some(scaling) = body.get("scalingConfig") {
            ng.scaling_config = build_scaling_config(Some(scaling));
            params.push(("ScalingConfig".to_string(), scaling.to_string()));
        }
        if let Some(labels) = body.get("labels") {
            if let Some(add) = labels.get("addOrUpdateLabels").and_then(|v| v.as_object()) {
                let map = ng.labels.as_object_mut();
                if let Some(map) = map {
                    for (k, v) in add {
                        map.insert(k.clone(), v.clone());
                    }
                }
            }
            params.push(("LabelsToAdd".to_string(), labels.to_string()));
        }
        if let Some(update_config) = body.get("updateConfig") {
            ng.update_config = build_nodegroup_update_config(Some(update_config));
            params.push(("MaxUnavailable".to_string(), update_config.to_string()));
        }
        ng.modified_at = Utc::now();

        let update = new_update("ConfigUpdate", params);
        let out = update_json(&update);
        ng.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    fn update_nodegroup_version(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let ng = state
            .nodegroups
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_nodegroup(name))?;

        let mut params = Vec::new();
        if let Some(version) = body.get("version").and_then(|v| v.as_str()) {
            ng.version = version.to_string();
            params.push(("Version".to_string(), version.to_string()));
        }
        if let Some(release) = body.get("releaseVersion").and_then(|v| v.as_str()) {
            ng.release_version = release.to_string();
            params.push(("ReleaseVersion".to_string(), release.to_string()));
        }
        ng.modified_at = Utc::now();

        let update = new_update("VersionUpdate", params);
        let out = update_json(&update);
        ng.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    // -----------------------------------------------------------------------
    // Fargate profiles
    // -----------------------------------------------------------------------

    fn create_fargate_profile(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("fargateProfileName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("fargateProfileName is required"))?
            .to_string();
        let pod_execution_role_arn = body
            .get("podExecutionRoleArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("podExecutionRoleArn is required"))?
            .to_string();

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        if state
            .fargate_profiles
            .get(cluster_name)
            .is_some_and(|m| m.contains_key(&name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!(
                    "FargateProfile already exists with name {name} and cluster name {cluster_name}"
                ),
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let arn = fargate_profile_arn(&region, &account_id, cluster_name, &name, &id);
        let profile = FargateProfile {
            name: name.clone(),
            arn,
            cluster_name: cluster_name.to_string(),
            pod_execution_role_arn,
            status: "CREATING".to_string(),
            created_at: Utc::now(),
            subnets: body.get("subnets").cloned().unwrap_or_else(|| json!([])),
            selectors: body.get("selectors").cloned().unwrap_or_else(|| json!([])),
            tags: parse_tag_map(body.get("tags")),
        };

        let out = fargate_profile_json(&profile);
        state
            .fargate_profiles
            .entry(cluster_name.to_string())
            .or_default()
            .insert(name, profile);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "fargateProfile": out }).to_string(),
        ))
    }

    fn describe_fargate_profile(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let profile = state
            .fargate_profiles
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_fargate_profile(name))?;
        if profile.status == "CREATING" {
            profile.status = "ACTIVE".to_string();
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "fargateProfile": fargate_profile_json(profile) }).to_string(),
        ))
    }

    fn list_fargate_profiles(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(cluster_name))?;
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let names: Vec<String> = state
            .fargate_profiles
            .get(cluster_name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let (page, token) = paginate_checked(&names, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "fargateProfileNames": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_fargate_profile(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let mut profile = state
            .fargate_profiles
            .get_mut(cluster_name)
            .and_then(|m| m.remove(name))
            .ok_or_else(not_found_fargate_profile(name))?;
        profile.status = "DELETING".to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "fargateProfile": fargate_profile_json(&profile) }).to_string(),
        ))
    }

    // -----------------------------------------------------------------------
    // Add-ons
    // -----------------------------------------------------------------------

    fn create_addon(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("addonName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("addonName is required"))?
            .to_string();

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // The parent cluster must exist; AWS returns ResourceNotFoundException.
        let cluster_version = state
            .clusters
            .get(cluster_name)
            .ok_or_else(not_found_cluster(cluster_name))?
            .version
            .clone();
        if state
            .addons
            .get(cluster_name)
            .is_some_and(|m| m.contains_key(&name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!("Addon already exists with name {name} and cluster name {cluster_name}"),
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let arn = addon_arn(&region, &account_id, cluster_name, &name, &id);
        let now = Utc::now();
        let addon_version = body
            .get("addonVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| default_addon_version(&name, &cluster_version));
        let namespace = body
            .get("namespaceConfig")
            .and_then(|v| v.get("namespace"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let pod_identity_associations = build_pod_identity_association_arns(
            &region,
            &account_id,
            cluster_name,
            body.get("podIdentityAssociations"),
        );

        let addon = Addon {
            name: name.clone(),
            arn,
            cluster_name: cluster_name.to_string(),
            addon_version,
            status: "CREATING".to_string(),
            created_at: now,
            modified_at: now,
            service_account_role_arn: body
                .get("serviceAccountRoleArn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            configuration_values: body
                .get("configurationValues")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            namespace,
            pod_identity_associations,
            tags: parse_tag_map(body.get("tags")),
            updates: Default::default(),
        };

        let out = addon_json(&addon);
        state
            .addons
            .entry(cluster_name.to_string())
            .or_default()
            .insert(name, addon);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "addon": out }).to_string(),
        ))
    }

    fn describe_addon(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let addon = state
            .addons
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_addon(name))?;
        // Settle CREATING -> ACTIVE on first describe.
        if addon.status == "CREATING" {
            addon.status = "ACTIVE".to_string();
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "addon": addon_json(addon) }).to_string(),
        ))
    }

    fn list_addons(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(cluster_name))?;
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let names: Vec<String> = state
            .addons
            .get(cluster_name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let (page, token) = paginate_checked(&names, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "addons": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_addon(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let mut addon = state
            .addons
            .get_mut(cluster_name)
            .and_then(|m| m.remove(name))
            .ok_or_else(not_found_addon(name))?;
        addon.status = "DELETING".to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "addon": addon_json(&addon) }).to_string(),
        ))
    }

    fn update_addon(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let region = req.region.clone();
        let account_id = req.account_id.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let addon = state
            .addons
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_addon(name))?;

        let mut params = Vec::new();
        if let Some(version) = body.get("addonVersion").and_then(|v| v.as_str()) {
            addon.addon_version = version.to_string();
            params.push(("AddonVersion".to_string(), version.to_string()));
        }
        if let Some(role) = body.get("serviceAccountRoleArn").and_then(|v| v.as_str()) {
            addon.service_account_role_arn = Some(role.to_string());
            params.push(("ServiceAccountRoleArn".to_string(), role.to_string()));
        }
        if let Some(cfg) = body.get("configurationValues").and_then(|v| v.as_str()) {
            addon.configuration_values = Some(cfg.to_string());
            params.push(("ConfigurationValues".to_string(), cfg.to_string()));
        }
        if let Some(resolve) = body.get("resolveConflicts").and_then(|v| v.as_str()) {
            params.push(("ResolveConflicts".to_string(), resolve.to_string()));
        }
        if let Some(assocs) = body.get("podIdentityAssociations") {
            addon.pod_identity_associations = build_pod_identity_association_arns(
                &region,
                &account_id,
                cluster_name,
                Some(assocs),
            );
        }
        addon.modified_at = Utc::now();

        let update = new_update("AddonUpdate", params);
        let out = update_json(&update);
        addon.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    fn describe_addon_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let addon_filter = req.query_params.get("addonName").cloned();
        let k8s_version = req
            .query_params
            .get("kubernetesVersion")
            .cloned()
            .unwrap_or_else(|| DEFAULT_K8S_VERSION.to_string());

        let catalog = addon_catalog(&k8s_version);
        let filtered: Vec<Value> = catalog
            .into_iter()
            .filter(|a| addon_filter.as_deref().is_none_or(|f| a["addonName"] == f))
            .collect();

        let (page, token) = paginate_checked(&filtered, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "addons": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn describe_addon_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let addon_name = req
            .query_params
            .get("addonName")
            .cloned()
            .ok_or_else(|| invalid_parameter("addonName is required"))?;
        let addon_version = req
            .query_params
            .get("addonVersion")
            .cloned()
            .ok_or_else(|| invalid_parameter("addonVersion is required"))?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({
                "addonName": addon_name,
                "addonVersion": addon_version,
                "configurationSchema": addon_configuration_schema(&addon_name),
                "podIdentityConfiguration": pod_identity_configuration(&addon_name),
            })
            .to_string(),
        ))
    }
}

/// Persist the current EKS state as a snapshot. Shared by
/// `EksService::save_snapshot` and the CFN provisioner's persist hook.
pub async fn save_eks_snapshot(
    state: &SharedEksState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = EksSnapshot {
        schema_version: EKS_SNAPSHOT_SCHEMA_VERSION,
        accounts: state.read().clone(),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write eks snapshot"),
        Err(err) => tracing::error!(%err, "eks snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for EksService {
    fn service_name(&self) -> &str {
        "eks"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, args) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let mutates = matches!(
            action,
            "CreateCluster"
                | "DeleteCluster"
                | "UpdateClusterConfig"
                | "UpdateClusterVersion"
                | "TagResource"
                | "UntagResource"
                // Describe settles CREATING -> ACTIVE and IN_PROGRESS updates,
                // which mutates persisted state.
                | "DescribeCluster"
                | "DescribeUpdate"
                | "CreateNodegroup"
                | "DeleteNodegroup"
                | "UpdateNodegroupConfig"
                | "UpdateNodegroupVersion"
                | "DescribeNodegroup"
                | "CreateFargateProfile"
                | "DeleteFargateProfile"
                | "DescribeFargateProfile"
                | "CreateAddon"
                | "DeleteAddon"
                | "UpdateAddon"
                | "DescribeAddon"
        );

        let result = match (action, &args) {
            ("CreateCluster", _) => self.create_cluster(&req),
            ("ListClusters", _) => self.list_clusters(&req),
            ("DescribeCluster", PathArgs::Name(n)) => self.describe_cluster(&req, n),
            ("DeleteCluster", PathArgs::Name(n)) => self.delete_cluster(&req, n),
            ("UpdateClusterConfig", PathArgs::Name(n)) => self.update_cluster_config(&req, n),
            ("UpdateClusterVersion", PathArgs::Name(n)) => self.update_cluster_version(&req, n),
            ("ListUpdates", PathArgs::Name(n)) => self.list_updates(&req, n),
            ("DescribeUpdate", PathArgs::Update { name, update_id }) => {
                self.describe_update(&req, name, update_id)
            }
            ("TagResource", PathArgs::Arn(a)) => self.tag_resource(&req, a),
            ("UntagResource", PathArgs::Arn(a)) => self.untag_resource(&req, a),
            ("ListTagsForResource", PathArgs::Arn(a)) => self.list_tags_for_resource(&req, a),
            ("CreateNodegroup", PathArgs::Cluster(c)) => self.create_nodegroup(&req, c),
            ("ListNodegroups", PathArgs::Cluster(c)) => self.list_nodegroups(&req, c),
            ("DescribeNodegroup", PathArgs::ClusterChild { cluster, name }) => {
                self.describe_nodegroup(&req, cluster, name)
            }
            ("DeleteNodegroup", PathArgs::ClusterChild { cluster, name }) => {
                self.delete_nodegroup(&req, cluster, name)
            }
            ("UpdateNodegroupConfig", PathArgs::ClusterChild { cluster, name }) => {
                self.update_nodegroup_config(&req, cluster, name)
            }
            ("UpdateNodegroupVersion", PathArgs::ClusterChild { cluster, name }) => {
                self.update_nodegroup_version(&req, cluster, name)
            }
            ("CreateFargateProfile", PathArgs::Cluster(c)) => self.create_fargate_profile(&req, c),
            ("ListFargateProfiles", PathArgs::Cluster(c)) => self.list_fargate_profiles(&req, c),
            ("DescribeFargateProfile", PathArgs::ClusterChild { cluster, name }) => {
                self.describe_fargate_profile(&req, cluster, name)
            }
            ("DeleteFargateProfile", PathArgs::ClusterChild { cluster, name }) => {
                self.delete_fargate_profile(&req, cluster, name)
            }
            ("CreateAddon", PathArgs::Cluster(c)) => self.create_addon(&req, c),
            ("ListAddons", PathArgs::Cluster(c)) => self.list_addons(&req, c),
            ("DescribeAddon", PathArgs::ClusterChild { cluster, name }) => {
                self.describe_addon(&req, cluster, name)
            }
            ("DeleteAddon", PathArgs::ClusterChild { cluster, name }) => {
                self.delete_addon(&req, cluster, name)
            }
            ("UpdateAddon", PathArgs::ClusterChild { cluster, name }) => {
                self.update_addon(&req, cluster, name)
            }
            ("DescribeAddonVersions", _) => self.describe_addon_versions(&req),
            ("DescribeAddonConfiguration", _) => self.describe_addon_configuration(&req),
            _ => Err(AwsServiceError::action_not_implemented("eks", action)),
        };

        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        EKS_ACTIONS
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", msg)
}

fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// Validate the shared `maxResults` query param (range 1-100) used by the list
/// operations, returning the effective page size.
fn validate_max_results(req: &AwsRequest) -> Result<usize, AwsServiceError> {
    match req.query_params.get("maxResults") {
        Some(raw) => {
            let n: i64 = raw
                .parse()
                .map_err(|_| invalid_parameter("maxResults must be an integer"))?;
            if !(1..=100).contains(&n) {
                return Err(invalid_parameter("maxResults must be between 1 and 100"));
            }
            Ok(n as usize)
        }
        None => Ok(100),
    }
}

fn validate_cluster_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 100 {
        return Err(invalid_parameter("name must be 1-100 characters"));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphanumeric() {
        return Err(invalid_parameter(
            "name must start with an alphanumeric character",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
    {
        return Err(invalid_parameter(
            "name must match ^[0-9A-Za-z][A-Za-z0-9\\-_]*$",
        ));
    }
    Ok(())
}

fn not_found_cluster(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No cluster found for name: {name}."),
        )
    }
}

fn not_found_update(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No update found for id: {id}."),
        )
    }
}

fn not_found_nodegroup(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No node group found for name: {name}."),
        )
    }
}

fn not_found_fargate_profile(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No Fargate Profile found with name: {name}."),
        )
    }
}

fn not_found_addon(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No addon found for name: {name}."),
        )
    }
}

fn not_found_arn(arn: &str) -> impl Fn() -> AwsServiceError + 'static {
    let arn = arn.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "NotFoundException",
            format!("Resource not found: {arn}"),
        )
    }
}

/// Extract the cluster name from an EKS cluster ARN, rejecting malformed or
/// non-cluster ARNs with a BadRequestException (matching the tag ops' error set).
fn cluster_name_from_arn(arn: &str) -> Result<String, AwsServiceError> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[2] != "eks" {
        return Err(bad_request(format!("Invalid EKS ARN: {arn}")));
    }
    let resource = parts[5];
    let name = resource
        .strip_prefix("cluster/")
        .filter(|n| !n.is_empty())
        .ok_or_else(|| bad_request(format!("Unsupported resource ARN: {arn}")))?;
    Ok(name.to_string())
}

fn parse_tag_map(v: Option<&Value>) -> crate::state::TagMap {
    let mut out = crate::state::TagMap::new();
    if let Some(obj) = v.and_then(|v| v.as_object()) {
        for (k, val) in obj {
            if let Some(s) = val.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// Parse a repeated query-string key (`?tagKeys=a&tagKeys=b`) into decoded values.
fn parse_multi_query(raw_query: &str, key: &str) -> Vec<String> {
    let prefix = format!("{key}=");
    raw_query
        .split('&')
        .filter(|pair| pair.starts_with(&prefix))
        .map(|pair| decode(&pair[prefix.len()..]))
        .collect()
}

fn new_update(update_type: &str, params: Vec<(String, String)>) -> Update {
    Update {
        id: uuid::Uuid::new_v4().to_string(),
        status: "InProgress".to_string(),
        type_: update_type.to_string(),
        params,
        created_at: Utc::now(),
    }
}

fn timestamp_to_number(t: DateTime<Utc>) -> Value {
    let secs = t.timestamp() as f64;
    let frac = t.timestamp_subsec_millis() as f64 / 1000.0;
    Value::from(secs + frac)
}

/// Recover the synthetic cluster id from a stored endpoint URL so describe/delete
/// can re-derive the OIDC issuer without persisting the id separately.
fn arn_cluster_id(endpoint: &str) -> String {
    endpoint
        .strip_prefix("https://")
        .and_then(|s| s.split('.').next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default()
}

fn default_ca_data() -> String {
    // A stable placeholder base64 blob; real clusters return a PEM-encoded CA.
    "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg==".to_string()
}

fn build_vpc_config_response(req: &Value, id: &str) -> Value {
    let subnet_ids = req.get("subnetIds").cloned().unwrap_or_else(|| json!([]));
    let security_group_ids = req
        .get("securityGroupIds")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let public_access_cidrs = req
        .get("publicAccessCidrs")
        .cloned()
        .unwrap_or_else(|| json!(["0.0.0.0/0"]));
    let endpoint_public = req
        .get("endpointPublicAccess")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let endpoint_private = req
        .get("endpointPrivateAccess")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut out = json!({
        "subnetIds": subnet_ids,
        "securityGroupIds": security_group_ids,
        "clusterSecurityGroupId": format!("sg-{}", &id.replace('-', "")[..17.min(id.replace('-', "").len())]),
        "vpcId": format!("vpc-{}", &id.replace('-', "")[..17.min(id.replace('-', "").len())]),
        "endpointPublicAccess": endpoint_public,
        "endpointPrivateAccess": endpoint_private,
        "publicAccessCidrs": public_access_cidrs,
    });
    if let Some(mode) = req.get("controlPlaneEgressMode") {
        out["controlPlaneEgressMode"] = mode.clone();
    }
    out
}

fn build_k8s_network_config(req: Option<&Value>) -> Value {
    let ip_family = req
        .and_then(|v| v.get("ipFamily"))
        .and_then(|v| v.as_str())
        .unwrap_or("ipv4")
        .to_string();
    let service_cidr = req
        .and_then(|v| v.get("serviceIpv4Cidr"))
        .and_then(|v| v.as_str())
        .unwrap_or("10.100.0.0/16")
        .to_string();
    json!({
        "serviceIpv4Cidr": service_cidr,
        "ipFamily": ip_family,
    })
}

fn build_logging(req: Option<&Value>) -> Value {
    // If the caller supplies a Logging block, echo its clusterLogging; else
    // default to all types disabled (what AWS reports for a fresh cluster).
    if let Some(setups) = req
        .and_then(|v| v.get("clusterLogging"))
        .and_then(|v| v.as_array())
    {
        return json!({ "clusterLogging": setups });
    }
    json!({
        "clusterLogging": [{
            "types": LOG_TYPES,
            "enabled": false,
        }],
    })
}

fn cluster_json(c: &Cluster, id: &str) -> Value {
    json!({
        "name": c.name,
        "arn": c.arn,
        "createdAt": timestamp_to_number(c.created_at),
        "version": c.version,
        "endpoint": c.endpoint,
        "roleArn": c.role_arn,
        "resourcesVpcConfig": c.resources_vpc_config,
        "kubernetesNetworkConfig": c.kubernetes_network_config,
        "logging": c.logging,
        "identity": {
            "oidc": {
                "issuer": format!("https://oidc.eks.amazonaws.com/id/{}", id.to_uppercase()),
            },
        },
        "status": c.status,
        "certificateAuthority": { "data": c.certificate_authority_data },
        "platformVersion": c.platform_version,
        "tags": c.tags,
        "health": { "issues": [] },
    })
}

fn update_json(u: &Update) -> Value {
    let params: Vec<Value> = u
        .params
        .iter()
        .map(|(t, v)| json!({ "type": t, "value": v }))
        .collect();
    json!({
        "id": u.id,
        "status": u.status,
        "type": u.type_,
        "params": params,
        "createdAt": timestamp_to_number(u.created_at),
        "errors": [],
    })
}

/// Build a `NodegroupScalingConfig`, defaulting to AWS's create-time defaults
/// (min 1 / max 2 / desired 2) for any member the caller omits.
fn build_scaling_config(req: Option<&Value>) -> Value {
    let min = req
        .and_then(|v| v.get("minSize"))
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    let max = req
        .and_then(|v| v.get("maxSize"))
        .and_then(|v| v.as_i64())
        .unwrap_or(2);
    let desired = req
        .and_then(|v| v.get("desiredSize"))
        .and_then(|v| v.as_i64())
        .unwrap_or(2);
    json!({ "minSize": min, "maxSize": max, "desiredSize": desired })
}

/// Build a `NodegroupUpdateConfig`, defaulting `maxUnavailable` to 1.
fn build_nodegroup_update_config(req: Option<&Value>) -> Value {
    if let Some(pct) = req
        .and_then(|v| v.get("maxUnavailablePercentage"))
        .and_then(|v| v.as_i64())
    {
        return json!({ "maxUnavailablePercentage": pct });
    }
    let max_unavailable = req
        .and_then(|v| v.get("maxUnavailable"))
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    json!({ "maxUnavailable": max_unavailable })
}

fn nodegroup_json(n: &Nodegroup) -> Value {
    let mut out = json!({
        "nodegroupName": n.name,
        "nodegroupArn": n.arn,
        "clusterName": n.cluster_name,
        "version": n.version,
        "releaseVersion": n.release_version,
        "createdAt": timestamp_to_number(n.created_at),
        "modifiedAt": timestamp_to_number(n.modified_at),
        "status": n.status,
        "capacityType": n.capacity_type,
        "scalingConfig": n.scaling_config,
        "instanceTypes": n.instance_types,
        "subnets": n.subnets,
        "amiType": n.ami_type,
        "nodeRole": n.node_role,
        "labels": n.labels,
        "taints": n.taints,
        "resources": {
            "autoScalingGroups": [{ "name": n.asg_name }],
        },
        "diskSize": n.disk_size,
        "health": { "issues": [] },
        "updateConfig": n.update_config,
        "tags": n.tags,
    });
    if let Some(ra) = &n.remote_access {
        out["remoteAccess"] = ra.clone();
    }
    if let Some(lt) = &n.launch_template {
        out["launchTemplate"] = lt.clone();
    }
    out
}

fn fargate_profile_json(p: &FargateProfile) -> Value {
    json!({
        "fargateProfileName": p.name,
        "fargateProfileArn": p.arn,
        "clusterName": p.cluster_name,
        "createdAt": timestamp_to_number(p.created_at),
        "podExecutionRoleArn": p.pod_execution_role_arn,
        "subnets": p.subnets,
        "selectors": p.selectors,
        "status": p.status,
        "tags": p.tags,
        "health": { "issues": [] },
    })
}

/// The AWS default add-on version for a well-known add-on at a given cluster
/// version. Falls back to a generic `v1.0.0-eksbuild.1` for unknown add-ons so
/// CreateAddon always echoes a plausible version even without an explicit one.
fn default_addon_version(addon_name: &str, _cluster_version: &str) -> String {
    match addon_name {
        "vpc-cni" => "v1.18.3-eksbuild.2".to_string(),
        "coredns" => "v1.11.1-eksbuild.9".to_string(),
        "kube-proxy" => "v1.31.0-eksbuild.2".to_string(),
        "aws-ebs-csi-driver" => "v1.35.0-eksbuild.1".to_string(),
        "aws-efs-csi-driver" => "v2.1.0-eksbuild.1".to_string(),
        _ => "v1.0.0-eksbuild.1".to_string(),
    }
}

/// Turn the request's `podIdentityAssociations` (structs of
/// `{serviceAccount, roleArn}`) into the association ARNs echoed back on the
/// add-on as a StringList.
fn build_pod_identity_association_arns(
    region: &str,
    account_id: &str,
    cluster: &str,
    req: Option<&Value>,
) -> Vec<String> {
    let Some(list) = req.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    list.iter()
        .map(|_| {
            let id = uuid::Uuid::new_v4().to_string().replace('-', "");
            pod_identity_association_arn(region, account_id, cluster, &id[..17.min(id.len())])
        })
        .collect()
}

fn addon_json(a: &Addon) -> Value {
    let mut out = json!({
        "addonName": a.name,
        "clusterName": a.cluster_name,
        "status": a.status,
        "addonVersion": a.addon_version,
        "addonArn": a.arn,
        "createdAt": timestamp_to_number(a.created_at),
        "modifiedAt": timestamp_to_number(a.modified_at),
        "tags": a.tags,
        "health": { "issues": [] },
    });
    if let Some(role) = &a.service_account_role_arn {
        out["serviceAccountRoleArn"] = Value::String(role.clone());
    }
    if let Some(cfg) = &a.configuration_values {
        out["configurationValues"] = Value::String(cfg.clone());
    }
    if let Some(ns) = &a.namespace {
        out["namespaceConfig"] = json!({ "namespace": ns });
    }
    if !a.pod_identity_associations.is_empty() {
        out["podIdentityAssociations"] = json!(a.pod_identity_associations);
    }
    out
}

/// A single `AddonVersionInfo` catalog entry.
fn addon_version_info(version: &str, cluster_version: &str, requires_config: bool) -> Value {
    json!({
        "addonVersion": version,
        "architecture": ["amd64", "arm64"],
        "computeTypes": ["ec2", "fargate"],
        "compatibilities": [{
            "clusterVersion": cluster_version,
            "platformVersions": ["*"],
            "defaultVersion": true,
        }],
        "requiresConfiguration": requires_config,
        "requiresIamPermissions": false,
    })
}

/// A plausible real-AWS add-on version catalog scoped to a cluster version,
/// returned by `DescribeAddonVersions`. Each entry is an `AddonInfo`.
fn addon_catalog(cluster_version: &str) -> Vec<Value> {
    let entry = |name: &str, atype: &str, ns: &str, requires_config: bool| -> Value {
        let versions = vec![addon_version_info(
            &default_addon_version(name, cluster_version),
            cluster_version,
            requires_config,
        )];
        json!({
            "addonName": name,
            "type": atype,
            "addonVersions": versions,
            "publisher": "eks",
            "owner": "aws",
            "defaultNamespace": ns,
        })
    };
    vec![
        entry("vpc-cni", "networking", "kube-system", false),
        entry("coredns", "networking", "kube-system", false),
        entry("kube-proxy", "networking", "kube-system", false),
        entry("aws-ebs-csi-driver", "storage", "kube-system", false),
        entry("aws-efs-csi-driver", "storage", "kube-system", false),
    ]
}

/// A JSON-schema string describing the configuration accepted by an add-on,
/// returned by `DescribeAddonConfiguration`.
fn addon_configuration_schema(addon_name: &str) -> String {
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": format!("{addon_name} configuration schema"),
        "additionalProperties": false,
        "properties": {
            "resources": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "limits": { "type": "object" },
                    "requests": { "type": "object" },
                },
            },
            "tolerations": { "type": "array" },
            "nodeSelector": { "type": "object" },
        },
    })
    .to_string()
}

/// The recommended pod-identity configuration for an add-on, returned by
/// `DescribeAddonConfiguration` as `podIdentityConfiguration`.
fn pod_identity_configuration(addon_name: &str) -> Value {
    match addon_name {
        "vpc-cni" => json!([{
            "serviceAccount": "aws-node",
            "recommendedManagedPolicies": ["arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"],
        }]),
        "aws-ebs-csi-driver" => json!([{
            "serviceAccount": "ebs-csi-controller-sa",
            "recommendedManagedPolicies": ["arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"],
        }]),
        "aws-efs-csi-driver" => json!([{
            "serviceAccount": "efs-csi-controller-sa",
            "recommendedManagedPolicies": ["arn:aws:iam::aws:policy/service-role/AmazonEFSCSIDriverPolicy"],
        }]),
        _ => json!([]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::HeaderMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    fn make_state() -> SharedEksState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("111122223333", "us-east-1", ""),
        ))
    }

    fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
        let (p, q) = match path.find('?') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (path, ""),
        };
        let path_segments: Vec<String> = p
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let query_params: HashMap<String, String> = q
            .split('&')
            .filter(|s| !s.is_empty())
            .filter_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                Some((k.to_string(), v.to_string()))
            })
            .collect();
        AwsRequest {
            service: "eks".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "111122223333".to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments,
            raw_path: p.to_string(),
            raw_query: q.to_string(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn create_body(name: &str) -> String {
        json!({
            "name": name,
            "roleArn": "arn:aws:iam::111122223333:role/eks-cluster",
            "resourcesVpcConfig": {
                "subnetIds": ["subnet-1", "subnet-2"],
                "endpointPublicAccess": true
            }
        })
        .to_string()
    }

    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = EksService::new(make_state());
        assert!(svc.snapshot_hook().is_none());
    }

    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = EksService::new(make_state()).with_snapshot_store(store);
        let hook = svc.snapshot_hook().expect("hook present when store set");
        hook().await;
    }

    #[tokio::test]
    async fn create_describe_round_trip_settles_active() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters",
                &create_body("demo"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["name"], "demo");
        assert_eq!(v["cluster"]["status"], "CREATING");
        assert_eq!(
            v["cluster"]["arn"],
            "arn:aws:eks:us-east-1:111122223333:cluster/demo"
        );
        assert_eq!(v["cluster"]["version"], DEFAULT_K8S_VERSION);

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/demo", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["status"], "ACTIVE");
        assert_eq!(
            v["cluster"]["resourcesVpcConfig"]["endpointPublicAccess"],
            true
        );
    }

    #[tokio::test]
    async fn create_rejects_duplicate() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("dup")))
            .await
            .unwrap();
        let err = svc
            .handle(make_request(Method::POST, "/clusters", &create_body("dup")))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    #[tokio::test]
    async fn describe_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(Method::GET, "/clusters/ghost", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn list_then_delete_cluster() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("c1")))
            .await
            .unwrap();
        let resp = svc
            .handle(make_request(Method::GET, "/clusters", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["clusters"], json!(["c1"]));

        let resp = svc
            .handle(make_request(Method::DELETE, "/clusters/c1", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["status"], "DELETING");

        let err = svc
            .handle(make_request(Method::GET, "/clusters/c1", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_version_and_describe_update() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("up")))
            .await
            .unwrap();
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/up/updates",
                &json!({ "version": "1.32" }).to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "VersionUpdate");
        assert_eq!(v["update"]["status"], "InProgress");
        let update_id = v["update"]["id"].as_str().unwrap().to_string();

        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/up/updates/{update_id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["status"], "Successful");

        // The cluster's version reflects the update.
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/up", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["version"], "1.32");

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/up/updates", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["updateIds"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tag_untag_round_trip() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("tg")))
            .await
            .unwrap();
        let arn = "arn:aws:eks:us-east-1:111122223333:cluster/tg";
        let encoded = arn.replace('/', "%2F");
        svc.handle(make_request(
            Method::POST,
            &format!("/tags/{encoded}"),
            &json!({ "tags": { "env": "prod" } }).to_string(),
        ))
        .await
        .unwrap();

        let resp = svc
            .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["tags"]["env"], "prod");

        svc.handle(make_request(
            Method::DELETE,
            &format!("/tags/{encoded}?tagKeys=env"),
            "",
        ))
        .await
        .unwrap();
        let resp = svc
            .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["tags"], json!({}));
    }

    // -----------------------------------------------------------------------
    // Node groups
    // -----------------------------------------------------------------------

    fn nodegroup_body(name: &str) -> String {
        json!({
            "nodegroupName": name,
            "nodeRole": "arn:aws:iam::111122223333:role/eks-node",
            "subnets": ["subnet-1", "subnet-2"],
            "scalingConfig": { "minSize": 1, "maxSize": 3, "desiredSize": 2 },
            "instanceTypes": ["t3.large"],
            "labels": { "team": "core" }
        })
        .to_string()
    }

    async fn create_cluster(svc: &EksService, name: &str) {
        svc.handle(make_request(Method::POST, "/clusters", &create_body(name)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn nodegroup_create_describe_list_delete() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/node-groups",
                &nodegroup_body("ng1"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroup"]["nodegroupName"], "ng1");
        assert_eq!(v["nodegroup"]["status"], "CREATING");
        assert_eq!(v["nodegroup"]["clusterName"], "c1");
        assert_eq!(v["nodegroup"]["scalingConfig"]["maxSize"], 3);
        assert!(v["nodegroup"]["nodegroupArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks:us-east-1:111122223333:nodegroup/c1/ng1/"));

        // Describe settles CREATING -> ACTIVE.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/node-groups/ng1",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroup"]["status"], "ACTIVE");
        assert_eq!(v["nodegroup"]["labels"]["team"], "core");

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1/node-groups", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroups"], json!(["ng1"]));

        let resp = svc
            .handle(make_request(
                Method::DELETE,
                "/clusters/c1/node-groups/ng1",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroup"]["status"], "DELETING");

        let err = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/node-groups/ng1",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn nodegroup_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/node-groups",
                &nodegroup_body("ng1"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn nodegroup_duplicate_is_in_use() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups",
            &nodegroup_body("ng1"),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/node-groups",
                &nodegroup_body("ng1"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    #[tokio::test]
    async fn nodegroup_cross_cluster_isolation() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        create_cluster(&svc, "c2").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups",
            &nodegroup_body("ng1"),
        ))
        .await
        .unwrap();

        // c2 sees no node groups, and can't describe c1's node group.
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c2/node-groups", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroups"], json!([]));

        let err = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c2/node-groups/ng1",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn nodegroup_update_config_and_version_have_own_updates() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups",
            &nodegroup_body("ng1"),
        ))
        .await
        .unwrap();

        // UpdateNodegroupVersion returns an Update.
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/node-groups/ng1/update-version",
                &json!({ "version": "1.30" }).to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "VersionUpdate");
        let update_id = v["update"]["id"].as_str().unwrap().to_string();

        // UpdateNodegroupConfig returns another Update.
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/node-groups/ng1/update-config",
            &json!({ "scalingConfig": { "minSize": 2, "maxSize": 5, "desiredSize": 3 } })
                .to_string(),
        ))
        .await
        .unwrap();

        // ListUpdates with nodegroupName returns the node group's updates.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/updates?nodegroupName=ng1",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["updateIds"].as_array().unwrap().len(), 2);

        // Cluster-scoped ListUpdates (no nodegroupName) is empty.
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1/updates", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["updateIds"].as_array().unwrap().len(), 0);

        // DescribeUpdate with nodegroupName settles the node group update.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/updates/{update_id}?nodegroupName=ng1"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["status"], "Successful");

        // The node group reflects the new version and scaling config.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/node-groups/ng1",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroup"]["version"], "1.30");
        assert_eq!(v["nodegroup"]["scalingConfig"]["maxSize"], 5);
    }

    // -----------------------------------------------------------------------
    // Fargate profiles
    // -----------------------------------------------------------------------

    fn fargate_body(name: &str) -> String {
        json!({
            "fargateProfileName": name,
            "podExecutionRoleArn": "arn:aws:iam::111122223333:role/eks-fargate",
            "subnets": ["subnet-1"],
            "selectors": [{ "namespace": "default", "labels": { "app": "web" } }]
        })
        .to_string()
    }

    #[tokio::test]
    async fn fargate_create_describe_list_delete() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/fargate-profiles",
                &fargate_body("fp1"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["fargateProfile"]["fargateProfileName"], "fp1");
        assert_eq!(v["fargateProfile"]["status"], "CREATING");
        assert!(v["fargateProfile"]["fargateProfileArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks:us-east-1:111122223333:fargateprofile/c1/fp1/"));

        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/fargate-profiles/fp1",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["fargateProfile"]["status"], "ACTIVE");
        assert_eq!(v["fargateProfile"]["selectors"][0]["namespace"], "default");

        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/fargate-profiles",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["fargateProfileNames"], json!(["fp1"]));

        let resp = svc
            .handle(make_request(
                Method::DELETE,
                "/clusters/c1/fargate-profiles/fp1",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["fargateProfile"]["status"], "DELETING");

        let err = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/fargate-profiles/fp1",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn fargate_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/fargate-profiles",
                &fargate_body("fp1"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn fargate_duplicate_is_in_use() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/fargate-profiles",
            &fargate_body("fp1"),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/fargate-profiles",
                &fargate_body("fp1"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    #[tokio::test]
    async fn unimplemented_subresource_falls_through() {
        // Access-entries routes are not implemented; the router must not
        // accidentally match them via the node-group/fargate-profile/addon arms.
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        let err = svc
            .handle(make_request(Method::GET, "/clusters/c1/access-entries", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "UnknownOperationException");
    }

    // -----------------------------------------------------------------------
    // Add-ons
    // -----------------------------------------------------------------------

    fn addon_body(name: &str) -> String {
        json!({
            "addonName": name,
            "addonVersion": "v1.18.3-eksbuild.2",
            "serviceAccountRoleArn": "arn:aws:iam::111122223333:role/eks-addon",
            "configurationValues": "{\"replicaCount\":2}",
            "tags": { "team": "core" }
        })
        .to_string()
    }

    #[tokio::test]
    async fn addon_create_describe_list_update_delete() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/addons",
                &addon_body("vpc-cni"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["addon"]["addonName"], "vpc-cni");
        assert_eq!(v["addon"]["status"], "CREATING");
        assert_eq!(v["addon"]["clusterName"], "c1");
        assert_eq!(v["addon"]["addonVersion"], "v1.18.3-eksbuild.2");
        assert_eq!(v["addon"]["configurationValues"], "{\"replicaCount\":2}");
        assert!(v["addon"]["addonArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks:us-east-1:111122223333:addon/c1/vpc-cni/"));

        // Describe settles CREATING -> ACTIVE.
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1/addons/vpc-cni", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["addon"]["status"], "ACTIVE");
        assert_eq!(v["addon"]["tags"]["team"], "core");

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1/addons", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["addons"], json!(["vpc-cni"]));

        // UpdateAddon mints a tracked Update, discoverable via addonName.
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/addons/vpc-cni/update",
                &json!({ "addonVersion": "v1.18.5-eksbuild.1" }).to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "AddonUpdate");
        assert_eq!(v["update"]["status"], "InProgress");
        let update_id = v["update"]["id"].as_str().unwrap().to_string();

        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/updates/{update_id}?addonName=vpc-cni"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["status"], "Successful");

        // Addon reflects the new version.
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1/addons/vpc-cni", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["addon"]["addonVersion"], "v1.18.5-eksbuild.1");

        // ListUpdates with addonName sees the update.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/updates?addonName=vpc-cni",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["updateIds"].as_array().unwrap().len(), 1);

        // Delete.
        let resp = svc
            .handle(make_request(
                Method::DELETE,
                "/clusters/c1/addons/vpc-cni",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["addon"]["status"], "DELETING");

        let err = svc
            .handle(make_request(Method::GET, "/clusters/c1/addons/vpc-cni", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn addon_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/addons",
                &addon_body("coredns"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn addon_duplicate_is_in_use() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/addons",
            &addon_body("coredns"),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/addons",
                &addon_body("coredns"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    #[tokio::test]
    async fn describe_addon_versions_catalog_is_non_empty() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(Method::GET, "/addons/supported-versions", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let addons = v["addons"].as_array().unwrap();
        assert!(addons.len() >= 5);
        let names: Vec<&str> = addons
            .iter()
            .map(|a| a["addonName"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"vpc-cni"));
        assert!(names.contains(&"coredns"));
        assert!(!addons[0]["addonVersions"].as_array().unwrap().is_empty());

        // Filtered by addonName.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/addons/supported-versions?addonName=coredns",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let addons = v["addons"].as_array().unwrap();
        assert_eq!(addons.len(), 1);
        assert_eq!(addons[0]["addonName"], "coredns");
    }

    #[tokio::test]
    async fn describe_addon_configuration_returns_schema() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/addons/configuration-schemas?addonName=vpc-cni&addonVersion=v1.18.3-eksbuild.2",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["addonName"], "vpc-cni");
        assert_eq!(v["addonVersion"], "v1.18.3-eksbuild.2");
        assert!(v["configurationSchema"]
            .as_str()
            .unwrap()
            .contains("$schema"));
        assert_eq!(
            v["podIdentityConfiguration"][0]["serviceAccount"],
            "aws-node"
        );
    }
}
