//! AWS EKS REST-JSON service handler.
//!
//! Implements the cluster control plane (Create/Describe/List/DeleteCluster,
//! UpdateClusterConfig, UpdateClusterVersion, Describe/ListUpdates,
//! Tag/Untag/ListTagsForResource) plus managed node groups
//! (Create/Describe/List/DeleteNodegroup, UpdateNodegroupConfig/Version) and
//! Fargate profiles (Create/Describe/List/DeleteFargateProfile), add-ons,
//! access entries and their access-policy associations, OIDC identity-provider
//! configs, and Pod Identity associations, all modeled as cluster
//! sub-resources.

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
    access_entry_arn, addon_arn, capability_arn, cluster_arn, eks_anywhere_subscription_arn,
    fargate_profile_arn, identity_provider_config_arn, nodegroup_arn, pod_identity_association_arn,
    AccessEntry, Addon, AssociatedPolicy, Capability, Cluster, EksAnywhereSubscription,
    EksSnapshot, FargateProfile, IdentityProviderConfig, Insight, InsightsRefresh, Nodegroup,
    PodIdentityAssociation, SharedEksState, Update, DEFAULT_K8S_VERSION,
    EKS_SNAPSHOT_SCHEMA_VERSION,
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
    "CreateAccessEntry",
    "DescribeAccessEntry",
    "ListAccessEntries",
    "DeleteAccessEntry",
    "UpdateAccessEntry",
    "AssociateAccessPolicy",
    "DisassociateAccessPolicy",
    "ListAssociatedAccessPolicies",
    "ListAccessPolicies",
    "AssociateIdentityProviderConfig",
    "DisassociateIdentityProviderConfig",
    "DescribeIdentityProviderConfig",
    "ListIdentityProviderConfigs",
    "CreatePodIdentityAssociation",
    "DeletePodIdentityAssociation",
    "DescribePodIdentityAssociation",
    "ListPodIdentityAssociations",
    "UpdatePodIdentityAssociation",
    "DescribeInsight",
    "ListInsights",
    "DescribeInsightsRefresh",
    "StartInsightsRefresh",
    "AssociateEncryptionConfig",
    "CancelUpdate",
    "DeregisterCluster",
    "RegisterCluster",
    "DescribeClusterVersions",
    "CreateCapability",
    "DeleteCapability",
    "DescribeCapability",
    "ListCapabilities",
    "UpdateCapability",
    "CreateEksAnywhereSubscription",
    "DeleteEksAnywhereSubscription",
    "DescribeEksAnywhereSubscription",
    "ListEksAnywhereSubscriptions",
    "UpdateEksAnywhereSubscription",
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
    /// A named access policy association on an access entry
    /// (`DisassociateAccessPolicy`).
    AccessPolicyChild {
        cluster: String,
        principal: String,
        policy_arn: String,
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
            // Access entries (sub-resources of a cluster, keyed by principalArn).
            (&Method::POST, ["clusters", c, "access-entries"]) => {
                Some(("CreateAccessEntry", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "access-entries"]) => {
                Some(("ListAccessEntries", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "access-entries", p]) => Some((
                "DescribeAccessEntry",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(p),
                },
            )),
            (&Method::DELETE, ["clusters", c, "access-entries", p]) => Some((
                "DeleteAccessEntry",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(p),
                },
            )),
            (&Method::POST, ["clusters", c, "access-entries", p]) => Some((
                "UpdateAccessEntry",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(p),
                },
            )),
            // Access-policy associations on an access entry.
            (&Method::POST, ["clusters", c, "access-entries", p, "access-policies"]) => Some((
                "AssociateAccessPolicy",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(p),
                },
            )),
            (&Method::GET, ["clusters", c, "access-entries", p, "access-policies"]) => Some((
                "ListAssociatedAccessPolicies",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(p),
                },
            )),
            (&Method::DELETE, ["clusters", c, "access-entries", p, "access-policies", policy]) => {
                Some((
                    "DisassociateAccessPolicy",
                    PathArgs::AccessPolicyChild {
                        cluster: decode(c),
                        principal: decode(p),
                        policy_arn: decode(policy),
                    },
                ))
            }
            // Access-policy catalogue (not scoped to a cluster).
            (&Method::GET, ["access-policies"]) => Some(("ListAccessPolicies", PathArgs::None)),
            // Identity-provider configs (sub-resources of a cluster). The
            // associate/disassociate/describe forms are POSTs to fixed action
            // sub-paths, carrying the config identity in the body.
            (&Method::POST, ["clusters", c, "identity-provider-configs", "associate"]) => Some((
                "AssociateIdentityProviderConfig",
                PathArgs::Cluster(decode(c)),
            )),
            (&Method::POST, ["clusters", c, "identity-provider-configs", "disassociate"]) => {
                Some((
                    "DisassociateIdentityProviderConfig",
                    PathArgs::Cluster(decode(c)),
                ))
            }
            (&Method::POST, ["clusters", c, "identity-provider-configs", "describe"]) => Some((
                "DescribeIdentityProviderConfig",
                PathArgs::Cluster(decode(c)),
            )),
            (&Method::GET, ["clusters", c, "identity-provider-configs"]) => {
                Some(("ListIdentityProviderConfigs", PathArgs::Cluster(decode(c))))
            }
            // Pod identity associations (sub-resources of a cluster, keyed by id).
            (&Method::POST, ["clusters", c, "pod-identity-associations"]) => {
                Some(("CreatePodIdentityAssociation", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "pod-identity-associations"]) => {
                Some(("ListPodIdentityAssociations", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "pod-identity-associations", id]) => Some((
                "DescribePodIdentityAssociation",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(id),
                },
            )),
            (&Method::DELETE, ["clusters", c, "pod-identity-associations", id]) => Some((
                "DeletePodIdentityAssociation",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(id),
                },
            )),
            (&Method::POST, ["clusters", c, "pod-identity-associations", id]) => Some((
                "UpdatePodIdentityAssociation",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(id),
                },
            )),
            // Insights (sub-resources of a cluster). List is a POST (it carries
            // a filter body); Describe is a GET keyed by insight id.
            (&Method::POST, ["clusters", c, "insights"]) => {
                Some(("ListInsights", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "insights", id]) => Some((
                "DescribeInsight",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(id),
                },
            )),
            (&Method::GET, ["clusters", c, "insights-refresh"]) => {
                Some(("DescribeInsightsRefresh", PathArgs::Cluster(decode(c))))
            }
            (&Method::POST, ["clusters", c, "insights-refresh"]) => {
                Some(("StartInsightsRefresh", PathArgs::Cluster(decode(c))))
            }
            // Encryption config association (mints a tracked cluster Update).
            (&Method::POST, ["clusters", c, "encryption-config", "associate"]) => {
                Some(("AssociateEncryptionConfig", PathArgs::Cluster(decode(c))))
            }
            // Cancel an in-progress update.
            (&Method::POST, ["clusters", name, "updates", update_id, "cancel-update"]) => Some((
                "CancelUpdate",
                PathArgs::Update {
                    name: decode(name),
                    update_id: decode(update_id),
                },
            )),
            // Connected (registered) clusters.
            (&Method::POST, ["cluster-registrations"]) => Some(("RegisterCluster", PathArgs::None)),
            (&Method::DELETE, ["cluster-registrations", name]) => {
                Some(("DeregisterCluster", PathArgs::Name(decode(name))))
            }
            // Kubernetes version catalogue (account/region-scoped).
            (&Method::GET, ["cluster-versions"]) => {
                Some(("DescribeClusterVersions", PathArgs::None))
            }
            // Capabilities (sub-resources of a cluster).
            (&Method::POST, ["clusters", c, "capabilities"]) => {
                Some(("CreateCapability", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "capabilities"]) => {
                Some(("ListCapabilities", PathArgs::Cluster(decode(c))))
            }
            (&Method::GET, ["clusters", c, "capabilities", n]) => Some((
                "DescribeCapability",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::DELETE, ["clusters", c, "capabilities", n]) => Some((
                "DeleteCapability",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            (&Method::POST, ["clusters", c, "capabilities", n]) => Some((
                "UpdateCapability",
                PathArgs::ClusterChild {
                    cluster: decode(c),
                    name: decode(n),
                },
            )),
            // EKS Anywhere subscriptions (account-scoped).
            (&Method::POST, ["eks-anywhere-subscriptions"]) => {
                Some(("CreateEksAnywhereSubscription", PathArgs::None))
            }
            (&Method::GET, ["eks-anywhere-subscriptions"]) => {
                Some(("ListEksAnywhereSubscriptions", PathArgs::None))
            }
            (&Method::GET, ["eks-anywhere-subscriptions", id]) => Some((
                "DescribeEksAnywhereSubscription",
                PathArgs::Name(decode(id)),
            )),
            (&Method::DELETE, ["eks-anywhere-subscriptions", id]) => {
                Some(("DeleteEksAnywhereSubscription", PathArgs::Name(decode(id))))
            }
            (&Method::POST, ["eks-anywhere-subscriptions", id]) => {
                Some(("UpdateEksAnywhereSubscription", PathArgs::Name(decode(id))))
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
            connector_config: None,
            encryption_config: None,
            access_config: build_access_config(body.get("accessConfig")),
            upgrade_policy: build_upgrade_policy(body.get("upgradePolicy")),
            compute_config: body.get("computeConfig").cloned(),
            storage_config: body.get("storageConfig").cloned(),
            zonal_shift_config: body.get("zonalShiftConfig").cloned(),
            remote_network_config: body.get("remoteNetworkConfig").cloned(),
            control_plane_scaling_config: body.get("controlPlaneScalingConfig").cloned(),
            deletion_protection: body.get("deletionProtection").and_then(|v| v.as_bool()),
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
        if !state.clusters.contains_key(name) {
            return Err(not_found_cluster(name)());
        }
        // Real EKS refuses to delete a cluster that still has managed node
        // groups or Fargate profiles attached; the caller must delete those
        // first.
        let has_nodegroups = state.nodegroups.get(name).is_some_and(|m| !m.is_empty());
        let has_fargate = state
            .fargate_profiles
            .get(name)
            .is_some_and(|m| !m.is_empty());
        if has_nodegroups || has_fargate {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!(
                    "Cluster has {} attached that must be deleted first",
                    if has_nodegroups {
                        "nodegroups"
                    } else {
                        "Fargate profiles"
                    }
                ),
            ));
        }
        // Cascade-remove every remaining cluster sub-resource so a same-name
        // recreate does not inherit the previous cluster's orphaned state.
        state.nodegroups.remove(name);
        state.fargate_profiles.remove(name);
        state.addons.remove(name);
        state.access_entries.remove(name);
        state.identity_provider_configs.remove(name);
        state.pod_identity_associations.remove(name);
        state.insights.remove(name);
        state.insights_refresh.remove(name);
        state.capabilities.remove(name);
        let mut cluster = state
            .clusters
            .remove(name)
            .expect("existence checked above");
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

        // A single UpdateClusterConfig call may carry more than one config
        // aspect; apply every field present (previously all but logging and
        // resourcesVpcConfig were silently dropped) and report the first-matched
        // aspect as the Update's type for a faithful DescribeUpdate round-trip.
        let mut update_type = "ConfigUpdate";
        let mut matched = false;
        let mut params: Vec<(String, String)> = Vec::new();
        macro_rules! mark {
            ($ty:expr) => {
                if !matched {
                    update_type = $ty;
                    matched = true;
                }
            };
        }

        if let Some(logging) = body.get("logging") {
            cluster.logging = build_logging(Some(logging));
            mark!("LoggingUpdate");
            params.push(("ClusterLogging".to_string(), logging.to_string()));
        }
        if let Some(vpc) = body.get("resourcesVpcConfig") {
            let id = arn_cluster_id(&cluster.endpoint);
            cluster.resources_vpc_config = build_vpc_config_response(vpc, &id);
            mark!("VpcConfigUpdate");
            params.push(("ResourcesVpcConfig".to_string(), vpc.to_string()));
        }
        if let Some(ac) = body.get("accessConfig") {
            cluster.access_config = build_access_config(Some(ac));
            mark!("AccessConfigUpdate");
            params.push((
                "AuthenticationMode".to_string(),
                cluster.access_config["authenticationMode"].to_string(),
            ));
        }
        if let Some(up) = body.get("upgradePolicy") {
            cluster.upgrade_policy = build_upgrade_policy(Some(up));
            mark!("UpgradePolicyUpdate");
            params.push((
                "SupportType".to_string(),
                cluster.upgrade_policy["supportType"].to_string(),
            ));
        }
        if let Some(kn) = body.get("kubernetesNetworkConfig") {
            cluster.kubernetes_network_config = build_k8s_network_config(Some(kn));
            mark!("VpcConfigUpdate");
            params.push(("KubernetesNetworkConfig".to_string(), kn.to_string()));
        }
        if let Some(cc) = body.get("computeConfig") {
            cluster.compute_config = Some(cc.clone());
            mark!("AutoModeUpdate");
            params.push(("ComputeConfig".to_string(), cc.to_string()));
        }
        if let Some(sc) = body.get("storageConfig") {
            cluster.storage_config = Some(sc.clone());
            mark!("AutoModeUpdate");
            params.push(("StorageConfig".to_string(), sc.to_string()));
        }
        if let Some(z) = body.get("zonalShiftConfig") {
            cluster.zonal_shift_config = Some(z.clone());
            mark!("ZonalShiftConfigUpdate");
            params.push(("ZonalShiftConfig".to_string(), z.to_string()));
        }
        if let Some(rn) = body.get("remoteNetworkConfig") {
            cluster.remote_network_config = Some(rn.clone());
            mark!("RemoteNetworkConfigUpdate");
            params.push(("RemoteNetworkConfig".to_string(), rn.to_string()));
        }
        if let Some(sp) = body.get("controlPlaneScalingConfig") {
            cluster.control_plane_scaling_config = Some(sp.clone());
            mark!("ControlPlaneScalingConfigUpdate");
            params.push(("ControlPlaneScalingConfig".to_string(), sp.to_string()));
        }
        if let Some(dp) = body.get("deletionProtection").and_then(|v| v.as_bool()) {
            cluster.deletion_protection = Some(dp);
            mark!("DeletionProtectionUpdate");
            params.push(("DeletionProtection".to_string(), dp.to_string()));
        }
        let _ = matched;

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
        validate_eks_arn(arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Resolve the ARN to whichever resource owns it (cluster or any
        // sub-resource) and tag that resource. Previously every non-cluster ARN
        // was rejected with a 400, so tagging a node group / add-on failed.
        let target = locate_tag_target(state, arn);
        let dest = tags_mut(state, &target);
        for (k, v) in tags.as_object().unwrap() {
            if let Some(v) = v.as_str() {
                dest.insert(k.clone(), v.to_string());
            }
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(&self, req: &AwsRequest, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        let keys = parse_multi_query(&req.raw_query, "tagKeys");
        validate_eks_arn(arn)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let target = locate_tag_target(state, arn);
        let dest = tags_mut(state, &target);
        for k in keys {
            dest.remove(&k);
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_eks_arn(arn)?;
        let accounts = self.state.read();
        let tags: serde_json::Map<String, Value> = accounts
            .get(&req.account_id)
            .map(|state| {
                let target = locate_tag_target(state, arn);
                tags_ref(state, &target)
                    .map(|t| {
                        t.iter()
                            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default();
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

    // -----------------------------------------------------------------------
    // Access entries / access policies
    // -----------------------------------------------------------------------

    fn create_access_entry(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let principal_arn = body
            .get("principalArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("principalArn is required"))?
            .to_string();

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // The parent cluster must exist; AWS returns ResourceNotFoundException.
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        if state
            .access_entries
            .get(cluster_name)
            .is_some_and(|m| m.contains_key(&principal_arn))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!(
                    "The specified access entry resource is already in use on this cluster: {principal_arn}"
                ),
            ));
        }

        let (principal_type, principal_name) = principal_parts(&principal_arn);
        let id = uuid::Uuid::new_v4().to_string();
        let arn = access_entry_arn(
            &region,
            &account_id,
            cluster_name,
            &principal_type,
            &principal_name,
            &id,
        );
        let now = Utc::now();
        let username = body
            .get("username")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| default_username(&principal_arn));
        let entry_type = body
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("STANDARD")
            .to_string();
        let kubernetes_groups = string_list(body.get("kubernetesGroups"));

        let entry = AccessEntry {
            principal_arn: principal_arn.clone(),
            cluster_name: cluster_name.to_string(),
            arn,
            kubernetes_groups,
            username,
            type_: entry_type,
            created_at: now,
            modified_at: now,
            tags: parse_tag_map(body.get("tags")),
            associated_policies: Vec::new(),
        };

        let out = access_entry_json(&entry);
        state
            .access_entries
            .entry(cluster_name.to_string())
            .or_default()
            .insert(principal_arn, entry);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "accessEntry": out }).to_string(),
        ))
    }

    fn describe_access_entry(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        principal_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(cluster_name))?;
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let entry = state
            .access_entries
            .get(cluster_name)
            .and_then(|m| m.get(principal_arn))
            .ok_or_else(not_found_access_entry(principal_arn))?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "accessEntry": access_entry_json(entry) }).to_string(),
        ))
    }

    fn list_access_entries(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let associated_policy = req.query_params.get("associatedPolicyArn").cloned();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(cluster_name))?;
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        // Optional `associatedPolicyArn` filter narrows the result to entries
        // that have that policy associated, mirroring the real API.
        let names: Vec<String> = state
            .access_entries
            .get(cluster_name)
            .map(|m| {
                m.values()
                    .filter(|e| {
                        associated_policy.as_deref().is_none_or(|p| {
                            e.associated_policies.iter().any(|ap| ap.policy_arn == p)
                        })
                    })
                    .map(|e| e.principal_arn.clone())
                    .collect()
            })
            .unwrap_or_default();
        let (page, token) = paginate_checked(&names, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "accessEntries": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_access_entry(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        principal_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        state
            .access_entries
            .get_mut(cluster_name)
            .and_then(|m| m.remove(principal_arn))
            .ok_or_else(not_found_access_entry(principal_arn))?;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn update_access_entry(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        principal_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let entry = state
            .access_entries
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(principal_arn))
            .ok_or_else(not_found_access_entry(principal_arn))?;

        if let Some(groups) = body.get("kubernetesGroups") {
            entry.kubernetes_groups = string_list(Some(groups));
        }
        if let Some(username) = body.get("username").and_then(|v| v.as_str()) {
            entry.username = username.to_string();
        }
        entry.modified_at = Utc::now();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "accessEntry": access_entry_json(entry) }).to_string(),
        ))
    }

    fn associate_access_policy(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        principal_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let policy_arn = body
            .get("policyArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("policyArn is required"))?
            .to_string();
        let access_scope = build_access_scope(body.get("accessScope"));

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let entry = state
            .access_entries
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(principal_arn))
            .ok_or_else(not_found_access_entry(principal_arn))?;

        let now = Utc::now();
        // Re-associating the same policy updates its scope in place, matching
        // the real API's idempotent upsert semantics.
        let associated = if let Some(existing) = entry
            .associated_policies
            .iter_mut()
            .find(|ap| ap.policy_arn == policy_arn)
        {
            existing.access_scope = access_scope;
            existing.modified_at = now;
            existing.clone()
        } else {
            let ap = AssociatedPolicy {
                policy_arn: policy_arn.clone(),
                access_scope,
                associated_at: now,
                modified_at: now,
            };
            entry.associated_policies.push(ap.clone());
            ap
        };
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({
                "clusterName": cluster_name,
                "principalArn": entry.principal_arn,
                "associatedAccessPolicy": associated_policy_json(&associated),
            })
            .to_string(),
        ))
    }

    fn disassociate_access_policy(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        principal_arn: &str,
        policy_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let entry = state
            .access_entries
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(principal_arn))
            .ok_or_else(not_found_access_entry(principal_arn))?;
        entry
            .associated_policies
            .retain(|ap| ap.policy_arn != policy_arn);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_associated_access_policies(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        principal_arn: &str,
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
        let entry = state
            .access_entries
            .get(cluster_name)
            .and_then(|m| m.get(principal_arn))
            .ok_or_else(not_found_access_entry(principal_arn))?;
        let policies: Vec<Value> = entry
            .associated_policies
            .iter()
            .map(associated_policy_json)
            .collect();
        let (page, token) = paginate_checked(&policies, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({
            "clusterName": cluster_name,
            "principalArn": entry.principal_arn,
            "associatedAccessPolicies": page,
        });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn list_access_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ListAccessPolicies is a read-only catalogue. Its `maxResults` carries
        // an `@range` (1-100) constraint the probe exercises, so an out-of-range
        // value is rejected. The nextToken, however, is tolerated (an
        // unparseable one restarts from the beginning) since the op's Smithy
        // contract declares no client error for it.
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let catalog = access_policy_catalog();
        let (page, token) = paginate_checked(&catalog, next_token.as_deref(), max_results)
            .unwrap_or_else(|_| (catalog.clone(), None));
        let mut out = json!({ "accessPolicies": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    // -----------------------------------------------------------------------
    // Identity-provider configs
    // -----------------------------------------------------------------------

    fn associate_identity_provider_config(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let oidc = body
            .get("oidc")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("oidc is required"))?;
        let name = oidc
            .get("identityProviderConfigName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("oidc.identityProviderConfigName is required"))?
            .to_string();
        let issuer_url = oidc
            .get("issuerUrl")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("oidc.issuerUrl is required"))?
            .to_string();
        let client_id = oidc
            .get("clientId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("oidc.clientId is required"))?
            .to_string();

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // The parent cluster must exist; AWS returns ResourceNotFoundException.
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        if state
            .identity_provider_configs
            .get(cluster_name)
            .is_some_and(|m| m.contains_key(&name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!(
                    "Identity provider config already exists with name {name} and cluster name {cluster_name}"
                ),
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let arn = identity_provider_config_arn(&region, &account_id, cluster_name, &name, &id);
        let config = IdentityProviderConfig {
            name: name.clone(),
            arn,
            cluster_name: cluster_name.to_string(),
            issuer_url,
            client_id,
            username_claim: str_field(oidc, "usernameClaim"),
            username_prefix: str_field(oidc, "usernamePrefix"),
            groups_claim: str_field(oidc, "groupsClaim"),
            groups_prefix: str_field(oidc, "groupsPrefix"),
            required_claims: oidc
                .get("requiredClaims")
                .cloned()
                .unwrap_or_else(|| json!({})),
            status: "CREATING".to_string(),
            tags: parse_tag_map(body.get("tags")),
        };
        state
            .identity_provider_configs
            .entry(cluster_name.to_string())
            .or_default()
            .insert(name, config.clone());

        // Associating an IdP config mints a tracked cluster-scoped Update, read
        // back through DescribeUpdate/ListUpdates like other config changes.
        let cluster = state.clusters.get_mut(cluster_name).unwrap();
        let update = new_update(
            "AssociateIdentityProviderConfig",
            vec![("IdentityProviderConfig".to_string(), oidc.to_string())],
        );
        let update_out = update_json(&update);
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": update_out, "tags": config.tags }).to_string(),
        ))
    }

    fn disassociate_identity_provider_config(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("identityProviderConfig")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("identityProviderConfig.name is required"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        state
            .identity_provider_configs
            .get_mut(cluster_name)
            .and_then(|m| m.remove(&name))
            .ok_or_else(not_found_identity_provider_config(&name))?;

        let cluster = state.clusters.get_mut(cluster_name).unwrap();
        let update = new_update(
            "DisassociateIdentityProviderConfig",
            vec![("IdentityProviderConfig".to_string(), name)],
        );
        let update_out = update_json(&update);
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": update_out }).to_string(),
        ))
    }

    fn describe_identity_provider_config(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("identityProviderConfig")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("identityProviderConfig.name is required"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let config = state
            .identity_provider_configs
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(&name))
            .ok_or_else(not_found_identity_provider_config(&name))?;
        // Settle CREATING -> ACTIVE on first describe.
        if config.status == "CREATING" {
            config.status = "ACTIVE".to_string();
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "identityProviderConfig": identity_provider_config_json(config) }).to_string(),
        ))
    }

    fn list_identity_provider_configs(
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
        let configs: Vec<Value> = state
            .identity_provider_configs
            .get(cluster_name)
            .map(|m| {
                m.keys()
                    .map(|n| json!({ "type": "oidc", "name": n }))
                    .collect()
            })
            .unwrap_or_default();
        let (page, token) = paginate_checked(&configs, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "identityProviderConfigs": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    // -----------------------------------------------------------------------
    // Pod identity associations
    // -----------------------------------------------------------------------

    fn create_pod_identity_association(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let namespace = body
            .get("namespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("namespace is required"))?
            .to_string();
        let service_account = body
            .get("serviceAccount")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("serviceAccount is required"))?
            .to_string();
        let role_arn = body
            .get("roleArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("roleArn is required"))?
            .to_string();

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // The parent cluster must exist; AWS returns ResourceNotFoundException.
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        // A given (namespace, serviceAccount) can only be associated once per
        // cluster; AWS returns ResourceInUseException for a duplicate.
        if state
            .pod_identity_associations
            .get(cluster_name)
            .is_some_and(|m| {
                m.values()
                    .any(|a| a.namespace == namespace && a.service_account == service_account)
            })
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!(
                    "Association already exists for namespace {namespace} and service account {service_account}"
                ),
            ));
        }

        let suffix = uuid::Uuid::new_v4().to_string().replace('-', "");
        let suffix = &suffix[..17.min(suffix.len())];
        let association_id = format!("a-{suffix}");
        let association_arn =
            pod_identity_association_arn(&region, &account_id, cluster_name, suffix);
        let target_role_arn = str_field(&body, "targetRoleArn");
        // AWS returns an externalId (for the target role's trust policy) only
        // when a cross-account targetRoleArn is supplied.
        let external_id = target_role_arn
            .as_ref()
            .map(|_| uuid::Uuid::new_v4().to_string().replace('-', ""));
        let now = Utc::now();
        let assoc = PodIdentityAssociation {
            cluster_name: cluster_name.to_string(),
            namespace,
            service_account,
            role_arn,
            association_arn,
            association_id: association_id.clone(),
            created_at: now,
            modified_at: now,
            disable_session_tags: body
                .get("disableSessionTags")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            target_role_arn,
            external_id,
            tags: parse_tag_map(body.get("tags")),
        };
        let out = pod_identity_association_json(&assoc);
        state
            .pod_identity_associations
            .entry(cluster_name.to_string())
            .or_default()
            .insert(association_id, assoc);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "association": out }).to_string(),
        ))
    }

    fn describe_pod_identity_association(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        association_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(cluster_name))?;
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let assoc = state
            .pod_identity_associations
            .get(cluster_name)
            .and_then(|m| m.get(association_id))
            .ok_or_else(not_found_pod_identity_association(association_id))?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "association": pod_identity_association_json(assoc) }).to_string(),
        ))
    }

    fn list_pod_identity_associations(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let namespace = req.query_params.get("namespace").cloned();
        let service_account = req.query_params.get("serviceAccount").cloned();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_cluster(cluster_name))?;
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let summaries: Vec<Value> = state
            .pod_identity_associations
            .get(cluster_name)
            .map(|m| {
                m.values()
                    .filter(|a| namespace.as_deref().is_none_or(|n| a.namespace == n))
                    .filter(|a| {
                        service_account
                            .as_deref()
                            .is_none_or(|s| a.service_account == s)
                    })
                    .map(pod_identity_association_summary_json)
                    .collect()
            })
            .unwrap_or_default();
        let (page, token) = paginate_checked(&summaries, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "associations": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_pod_identity_association(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        association_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let assoc = state
            .pod_identity_associations
            .get_mut(cluster_name)
            .and_then(|m| m.remove(association_id))
            .ok_or_else(not_found_pod_identity_association(association_id))?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "association": pod_identity_association_json(&assoc) }).to_string(),
        ))
    }

    fn update_pod_identity_association(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        association_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let assoc = state
            .pod_identity_associations
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(association_id))
            .ok_or_else(not_found_pod_identity_association(association_id))?;

        if let Some(role) = body.get("roleArn").and_then(|v| v.as_str()) {
            assoc.role_arn = role.to_string();
        }
        if let Some(target) = body.get("targetRoleArn").and_then(|v| v.as_str()) {
            assoc.target_role_arn = Some(target.to_string());
            // Mint an externalId for the target trust policy if we don't have one.
            if assoc.external_id.is_none() {
                assoc.external_id = Some(uuid::Uuid::new_v4().to_string().replace('-', ""));
            }
        }
        if let Some(disable) = body.get("disableSessionTags").and_then(|v| v.as_bool()) {
            assoc.disable_session_tags = disable;
        }
        assoc.modified_at = Utc::now();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "association": pod_identity_association_json(assoc) }).to_string(),
        ))
    }

    // -----------------------------------------------------------------------
    // Insights
    // -----------------------------------------------------------------------

    fn list_insights(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // ListInsights is a POST carrying the filter/pagination in its body.
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        // maxResults is constrained to 1..=100 by the model; reject out-of-range
        // values rather than silently clamping them.
        let max_results = match body.get("maxResults") {
            Some(v) => {
                let n = v
                    .as_i64()
                    .ok_or_else(|| invalid_parameter("maxResults must be an integer"))?;
                if !(1..=100).contains(&n) {
                    return Err(invalid_parameter("maxResults must be between 1 and 100"));
                }
                n as usize
            }
            None => 100,
        };
        let next_token = body
            .get("nextToken")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        // Optional category / status filters.
        let categories = string_list(body.get("filter").and_then(|f| f.get("categories")));
        let statuses = string_list(body.get("filter").and_then(|f| f.get("statuses")));

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let version = state
            .clusters
            .get(cluster_name)
            .ok_or_else(not_found_cluster(cluster_name))?
            .version
            .clone();
        let insights = state
            .insights
            .entry(cluster_name.to_string())
            .or_insert_with(|| {
                default_insights(&version)
                    .into_iter()
                    .map(|i| (i.id.clone(), i))
                    .collect()
            });
        let summaries: Vec<Value> = insights
            .values()
            .filter(|i| categories.is_empty() || categories.iter().any(|c| c == &i.category))
            .filter(|i| statuses.is_empty() || statuses.iter().any(|s| s == &i.status))
            .map(insight_summary_json)
            .collect();
        let (page, token) = paginate_checked(&summaries, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "insights": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn describe_insight(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let version = state
            .clusters
            .get(cluster_name)
            .ok_or_else(not_found_cluster(cluster_name))?
            .version
            .clone();
        let insights = state
            .insights
            .entry(cluster_name.to_string())
            .or_insert_with(|| {
                default_insights(&version)
                    .into_iter()
                    .map(|i| (i.id.clone(), i))
                    .collect()
            });
        let insight = insights.get(id).ok_or_else(not_found_insight(id))?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "insight": insight_json(insight) }).to_string(),
        ))
    }

    fn describe_insights_refresh(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        let refresh = state
            .insights_refresh
            .entry(cluster_name.to_string())
            .or_insert_with(|| InsightsRefresh {
                status: "COMPLETED".to_string(),
                started_at: Utc::now(),
                ended_at: Some(Utc::now()),
            });
        // An in-progress refresh settles to COMPLETED on first describe.
        if refresh.status == "IN_PROGRESS" {
            refresh.status = "COMPLETED".to_string();
            refresh.ended_at = Some(Utc::now());
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            insights_refresh_json(refresh).to_string(),
        ))
    }

    fn start_insights_refresh(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let version = state
            .clusters
            .get(cluster_name)
            .ok_or_else(not_found_cluster(cluster_name))?
            .version
            .clone();
        // A refresh regenerates the cluster's insights.
        state.insights.insert(
            cluster_name.to_string(),
            default_insights(&version)
                .into_iter()
                .map(|i| (i.id.clone(), i))
                .collect(),
        );
        let refresh = InsightsRefresh {
            status: "IN_PROGRESS".to_string(),
            started_at: Utc::now(),
            ended_at: None,
        };
        // StartInsightsRefresh's response carries only `message` + `status`;
        // the timestamps are read back through DescribeInsightsRefresh.
        let out = json!({
            "message": "Insights refresh started for the cluster.",
            "status": refresh.status,
        });
        state
            .insights_refresh
            .insert(cluster_name.to_string(), refresh);
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    // -----------------------------------------------------------------------
    // Cluster misc (encryption config, cancel update, register/deregister,
    // cluster versions)
    // -----------------------------------------------------------------------

    fn associate_encryption_config(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let encryption_config = body
            .get("encryptionConfig")
            .filter(|v| v.is_array())
            .cloned()
            .ok_or_else(|| invalid_parameter("encryptionConfig is required"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(cluster_name)
            .ok_or_else(not_found_cluster(cluster_name))?;
        cluster.encryption_config = Some(encryption_config.clone());

        let update = new_update(
            "AssociateEncryptionConfig",
            vec![(
                "EncryptionConfig".to_string(),
                encryption_config.to_string(),
            )],
        );
        let out = update_json(&update);
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    fn cancel_update(
        &self,
        req: &AwsRequest,
        name: &str,
        update_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cluster = state
            .clusters
            .get_mut(name)
            .ok_or_else(not_found_cluster(name))?;
        let update = cluster
            .updates
            .get_mut(update_id)
            .ok_or_else(not_found_update(update_id))?;
        update.status = "Cancelled".to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": update_json(update) }).to_string(),
        ))
    }

    fn register_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("name is required"))?
            .to_string();
        validate_cluster_name(&name)?;
        let connector = body
            .get("connectorConfig")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("connectorConfig is required"))?;
        let role_arn = connector
            .get("roleArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("connectorConfig.roleArn is required"))?
            .to_string();
        let provider = connector
            .get("provider")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("connectorConfig.provider is required"))?
            .to_string();

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.clusters.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!("Cluster already exists with name: {name}"),
            ));
        }

        let arn = cluster_arn(&region, &account_id, &name);
        let id = uuid::Uuid::new_v4().to_string();
        let activation_id = uuid::Uuid::new_v4().to_string();
        let activation_code = uuid::Uuid::new_v4().to_string().replace('-', "");
        let connector_config = json!({
            "activationId": activation_id,
            "activationCode": activation_code,
            "activationExpiry": timestamp_to_number(Utc::now() + chrono::Duration::hours(72)),
            "provider": provider,
            "roleArn": role_arn,
        });

        let cluster = Cluster {
            name: name.clone(),
            arn,
            version: String::new(),
            role_arn: String::new(),
            status: "PENDING".to_string(),
            created_at: Utc::now(),
            endpoint: String::new(),
            platform_version: "eks.1".to_string(),
            certificate_authority_data: String::new(),
            resources_vpc_config: json!({}),
            kubernetes_network_config: json!({}),
            logging: build_logging(None),
            tags: parse_tag_map(body.get("tags")),
            updates: Default::default(),
            connector_config: Some(connector_config),
            encryption_config: None,
            access_config: build_access_config(None),
            upgrade_policy: build_upgrade_policy(None),
            compute_config: None,
            storage_config: None,
            zonal_shift_config: None,
            remote_network_config: None,
            control_plane_scaling_config: None,
            deletion_protection: None,
        };
        let out = connected_cluster_json(&cluster, &id);
        state.clusters.insert(name, cluster);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": out }).to_string(),
        ))
    }

    fn deregister_cluster(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Deregister only applies to connected (registered) clusters.
        let is_connected = state
            .clusters
            .get(name)
            .map(|c| c.connector_config.is_some())
            .unwrap_or(false);
        if !is_connected {
            return Err(not_found_cluster(name)());
        }
        let mut cluster = state.clusters.remove(name).unwrap();
        cluster.status = "DELETING".to_string();
        let id = uuid::Uuid::new_v4().to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "cluster": connected_cluster_json(&cluster, &id) }).to_string(),
        ))
    }

    fn describe_cluster_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let next_token = req.query_params.get("nextToken").cloned();
        let max_results = match req.query_params.get("maxResults") {
            Some(raw) => {
                let n: i64 = raw
                    .parse()
                    .map_err(|_| invalid_parameter("maxResults must be an integer"))?;
                if !(1..=100).contains(&n) {
                    return Err(invalid_parameter("maxResults must be between 1 and 100"));
                }
                n as usize
            }
            None => 100,
        };
        // Enum query params reject values outside the model's declared members.
        if let Some(status) = req.query_params.get("status") {
            if !["unsupported", "standard_support", "extended_support"].contains(&status.as_str()) {
                return Err(invalid_parameter(format!("Invalid status: {status}")));
            }
        }
        if let Some(vs) = req.query_params.get("versionStatus") {
            if !["UNSUPPORTED", "STANDARD_SUPPORT", "EXTENDED_SUPPORT"].contains(&vs.as_str()) {
                return Err(invalid_parameter(format!("Invalid versionStatus: {vs}")));
            }
        }
        let default_only = req
            .query_params
            .get("defaultOnly")
            .map(|v| v == "true")
            .unwrap_or(false);
        // `includeAll=false` (the default) hides versions no longer in any
        // support window (UNSUPPORTED); `includeAll=true` returns the full set.
        let include_all = req
            .query_params
            .get("includeAll")
            .map(|v| v == "true")
            .unwrap_or(false);
        let status_filter = req.query_params.get("status").cloned();
        let version_status_filter = req.query_params.get("versionStatus").cloned();
        let version_filter = parse_multi_query(&req.raw_query, "clusterVersions");
        let cluster_type = req
            .query_params
            .get("clusterType")
            .cloned()
            .unwrap_or_else(|| "eks".to_string());

        let mut catalog: Vec<Value> = cluster_version_catalog(&cluster_type)
            .into_iter()
            .filter(|v| {
                version_filter.is_empty()
                    || version_filter
                        .iter()
                        .any(|f| v["clusterVersion"] == f.as_str())
            })
            .filter(|v| !default_only || v["defaultVersion"] == true)
            // The `status` (deprecated) and `versionStatus` params narrow to a
            // specific support tier; previously validated then ignored.
            .filter(|v| status_filter.as_deref().is_none_or(|s| v["status"] == s))
            .filter(|v| {
                version_status_filter
                    .as_deref()
                    .is_none_or(|s| v["versionStatus"] == s)
            })
            // Drop UNSUPPORTED rows unless the caller asked to include all, or
            // explicitly filtered to an unsupported tier.
            .filter(|v| {
                include_all
                    || status_filter.is_some()
                    || version_status_filter.is_some()
                    || v["versionStatus"] != "UNSUPPORTED"
            })
            .collect();
        // AWS reports the default version first; a stable sort keying non-default
        // rows after default ones preserves the ascending version order otherwise.
        catalog.sort_by_key(|v| v["defaultVersion"] != true);

        let (page, token) = paginate_checked(&catalog, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "clusterVersions": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    // -----------------------------------------------------------------------
    // Capabilities
    // -----------------------------------------------------------------------

    fn create_capability(
        &self,
        req: &AwsRequest,
        cluster_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("capabilityName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("capabilityName is required"))?
            .to_string();
        let type_ = body
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("type is required"))?
            .to_string();
        let role_arn = body
            .get("roleArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("roleArn is required"))?
            .to_string();

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.clusters.contains_key(cluster_name) {
            return Err(not_found_cluster(cluster_name)());
        }
        if state
            .capabilities
            .get(cluster_name)
            .is_some_and(|m| m.contains_key(&name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceInUseException",
                format!(
                    "Capability already exists with name {name} and cluster name {cluster_name}"
                ),
            ));
        }

        let id = uuid::Uuid::new_v4().to_string();
        let arn = capability_arn(&region, &account_id, cluster_name, &name, &id);
        let now = Utc::now();
        let capability = Capability {
            name: name.clone(),
            arn,
            cluster_name: cluster_name.to_string(),
            type_,
            role_arn,
            status: "CREATING".to_string(),
            version: "v1".to_string(),
            configuration: normalize_capability_configuration(body.get("configuration")),
            tags: parse_tag_map(body.get("tags")),
            created_at: now,
            modified_at: now,
            delete_propagation_policy: str_field(&body, "deletePropagationPolicy"),
        };
        let out = capability_json(&capability);
        state
            .capabilities
            .entry(cluster_name.to_string())
            .or_default()
            .insert(name, capability);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "capability": out }).to_string(),
        ))
    }

    fn describe_capability(
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
        let capability = state
            .capabilities
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_capability(name))?;
        if capability.status == "CREATING" {
            capability.status = "ACTIVE".to_string();
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "capability": capability_json(capability) }).to_string(),
        ))
    }

    fn list_capabilities(
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
        let summaries: Vec<Value> = state
            .capabilities
            .get(cluster_name)
            .map(|m| m.values().map(capability_summary_json).collect())
            .unwrap_or_default();
        let (page, token) = paginate_checked(&summaries, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "capabilities": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn delete_capability(
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
        let mut capability = state
            .capabilities
            .get_mut(cluster_name)
            .and_then(|m| m.remove(name))
            .ok_or_else(not_found_capability(name))?;
        capability.status = "DELETING".to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "capability": capability_json(&capability) }).to_string(),
        ))
    }

    fn update_capability(
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
        let capability = state
            .capabilities
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(not_found_capability(name))?;

        let mut params = Vec::new();
        if let Some(role) = body.get("roleArn").and_then(|v| v.as_str()) {
            capability.role_arn = role.to_string();
            params.push(("RoleArn".to_string(), role.to_string()));
        }
        if let Some(cfg) = normalize_capability_configuration(body.get("configuration")) {
            params.push(("Configuration".to_string(), cfg.to_string()));
            capability.configuration = Some(cfg);
        }
        capability.modified_at = Utc::now();

        // UpdateCapability returns a tracked cluster-scoped Update.
        let update = new_update("CapabilityUpdate", params);
        let out = update_json(&update);
        let cluster = state.clusters.get_mut(cluster_name).unwrap();
        cluster.updates.insert(update.id.clone(), update);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "update": out }).to_string(),
        ))
    }

    // -----------------------------------------------------------------------
    // EKS Anywhere subscriptions
    // -----------------------------------------------------------------------

    fn create_eks_anywhere_subscription(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let name = body
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_parameter("name is required"))?
            .to_string();
        // EksAnywhereSubscriptionName: length 1-100, ^[0-9A-Za-z][A-Za-z0-9\-_]*$.
        if name.is_empty() || name.len() > 100 {
            return Err(invalid_parameter("name must be 1-100 characters"));
        }
        if !name.starts_with(|c: char| c.is_ascii_alphanumeric())
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        {
            return Err(invalid_parameter(
                "name must match ^[0-9A-Za-z][A-Za-z0-9\\-_]*$",
            ));
        }
        if let Some(lt) = body.get("licenseType").and_then(|v| v.as_str()) {
            if lt != "Cluster" {
                return Err(invalid_parameter(format!("Invalid licenseType: {lt}")));
            }
        }
        let term = body
            .get("term")
            .filter(|v| v.is_object())
            .ok_or_else(|| invalid_parameter("term is required"))?;
        let term_duration = term.get("duration").and_then(|v| v.as_i64()).unwrap_or(12);
        // AWS EKS Anywhere subscriptions use month-based terms (12 or 36); bound
        // the value so a hostile `duration` can't overflow the date arithmetic
        // below (`DateTime + Duration` panics on overflow in all build profiles).
        if !(1..=120).contains(&term_duration) {
            return Err(invalid_parameter(format!(
                "term.duration must be between 1 and 120 months, got {term_duration}"
            )));
        }
        let term_unit = term
            .get("unit")
            .and_then(|v| v.as_str())
            .unwrap_or("MONTHS")
            .to_string();
        let license_quantity = body
            .get("licenseQuantity")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let license_type = body
            .get("licenseType")
            .and_then(|v| v.as_str())
            .unwrap_or("Cluster")
            .to_string();
        let auto_renew = body
            .get("autoRenew")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let region = req.region.clone();
        let account_id = req.account_id.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let raw = uuid::Uuid::new_v4().to_string().replace('-', "");
        let id = raw[..17.min(raw.len())].to_string();
        let arn = eks_anywhere_subscription_arn(&region, &account_id, &id);
        let now = Utc::now();
        let expiration = now
            .checked_add_signed(chrono::Duration::days(term_duration * 30))
            .ok_or_else(|| {
                invalid_parameter("term.duration produces an out-of-range expiration date")
            })?;
        let subscription = EksAnywhereSubscription {
            id: id.clone(),
            arn,
            name,
            created_at: now,
            effective_date: now,
            expiration_date: expiration,
            license_quantity,
            license_type,
            term_duration,
            term_unit,
            status: "ACTIVE".to_string(),
            auto_renew,
            tags: parse_tag_map(body.get("tags")),
        };
        let out = subscription_json(&subscription);
        state.eks_anywhere_subscriptions.insert(id, subscription);
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "subscription": out }).to_string(),
        ))
    }

    fn list_eks_anywhere_subscriptions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_results = validate_max_results(req)?;
        let next_token = req.query_params.get("nextToken").cloned();
        let accounts = self.state.read();
        let Some(state) = accounts.get(&req.account_id) else {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                json!({ "subscriptions": [] }).to_string(),
            ));
        };
        let subs: Vec<Value> = state
            .eks_anywhere_subscriptions
            .values()
            .map(subscription_json)
            .collect();
        let (page, token) = paginate_checked(&subs, next_token.as_deref(), max_results)
            .map_err(|_| invalid_parameter("Invalid nextToken"))?;
        let mut out = json!({ "subscriptions": page });
        if let Some(t) = token {
            out["nextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn describe_eks_anywhere_subscription(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_subscription(id))?;
        let subscription = state
            .eks_anywhere_subscriptions
            .get(id)
            .ok_or_else(not_found_subscription(id))?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "subscription": subscription_json(subscription) }).to_string(),
        ))
    }

    fn delete_eks_anywhere_subscription(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut subscription = state
            .eks_anywhere_subscriptions
            .remove(id)
            .ok_or_else(not_found_subscription(id))?;
        subscription.status = "DELETING".to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "subscription": subscription_json(&subscription) }).to_string(),
        ))
    }

    fn update_eks_anywhere_subscription(
        &self,
        req: &AwsRequest,
        id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let auto_renew = body
            .get("autoRenew")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| invalid_parameter("autoRenew is required"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let subscription = state
            .eks_anywhere_subscriptions
            .get_mut(id)
            .ok_or_else(not_found_subscription(id))?;
        subscription.auto_renew = auto_renew;
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "subscription": subscription_json(subscription) }).to_string(),
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
                | "CreateAccessEntry"
                | "DeleteAccessEntry"
                | "UpdateAccessEntry"
                | "AssociateAccessPolicy"
                | "DisassociateAccessPolicy"
                | "AssociateIdentityProviderConfig"
                | "DisassociateIdentityProviderConfig"
                // Describe settles the config's CREATING -> ACTIVE transition.
                | "DescribeIdentityProviderConfig"
                | "CreatePodIdentityAssociation"
                | "DeletePodIdentityAssociation"
                | "UpdatePodIdentityAssociation"
                // Insights are seeded/settled on read; refresh settles on describe.
                | "ListInsights"
                | "DescribeInsight"
                | "DescribeInsightsRefresh"
                | "StartInsightsRefresh"
                | "AssociateEncryptionConfig"
                | "CancelUpdate"
                | "RegisterCluster"
                | "DeregisterCluster"
                | "CreateCapability"
                | "DeleteCapability"
                | "UpdateCapability"
                | "DescribeCapability"
                | "CreateEksAnywhereSubscription"
                | "DeleteEksAnywhereSubscription"
                | "UpdateEksAnywhereSubscription"
                | "DescribeEksAnywhereSubscription"
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
            ("CreateAccessEntry", PathArgs::Cluster(c)) => self.create_access_entry(&req, c),
            ("ListAccessEntries", PathArgs::Cluster(c)) => self.list_access_entries(&req, c),
            ("DescribeAccessEntry", PathArgs::ClusterChild { cluster, name }) => {
                self.describe_access_entry(&req, cluster, name)
            }
            ("DeleteAccessEntry", PathArgs::ClusterChild { cluster, name }) => {
                self.delete_access_entry(&req, cluster, name)
            }
            ("UpdateAccessEntry", PathArgs::ClusterChild { cluster, name }) => {
                self.update_access_entry(&req, cluster, name)
            }
            ("AssociateAccessPolicy", PathArgs::ClusterChild { cluster, name }) => {
                self.associate_access_policy(&req, cluster, name)
            }
            ("ListAssociatedAccessPolicies", PathArgs::ClusterChild { cluster, name }) => {
                self.list_associated_access_policies(&req, cluster, name)
            }
            (
                "DisassociateAccessPolicy",
                PathArgs::AccessPolicyChild {
                    cluster,
                    principal,
                    policy_arn,
                },
            ) => self.disassociate_access_policy(&req, cluster, principal, policy_arn),
            ("ListAccessPolicies", _) => self.list_access_policies(&req),
            ("AssociateIdentityProviderConfig", PathArgs::Cluster(c)) => {
                self.associate_identity_provider_config(&req, c)
            }
            ("DisassociateIdentityProviderConfig", PathArgs::Cluster(c)) => {
                self.disassociate_identity_provider_config(&req, c)
            }
            ("DescribeIdentityProviderConfig", PathArgs::Cluster(c)) => {
                self.describe_identity_provider_config(&req, c)
            }
            ("ListIdentityProviderConfigs", PathArgs::Cluster(c)) => {
                self.list_identity_provider_configs(&req, c)
            }
            ("CreatePodIdentityAssociation", PathArgs::Cluster(c)) => {
                self.create_pod_identity_association(&req, c)
            }
            ("ListPodIdentityAssociations", PathArgs::Cluster(c)) => {
                self.list_pod_identity_associations(&req, c)
            }
            ("DescribePodIdentityAssociation", PathArgs::ClusterChild { cluster, name }) => {
                self.describe_pod_identity_association(&req, cluster, name)
            }
            ("DeletePodIdentityAssociation", PathArgs::ClusterChild { cluster, name }) => {
                self.delete_pod_identity_association(&req, cluster, name)
            }
            ("UpdatePodIdentityAssociation", PathArgs::ClusterChild { cluster, name }) => {
                self.update_pod_identity_association(&req, cluster, name)
            }
            ("ListInsights", PathArgs::Cluster(c)) => self.list_insights(&req, c),
            ("DescribeInsight", PathArgs::ClusterChild { cluster, name }) => {
                self.describe_insight(&req, cluster, name)
            }
            ("DescribeInsightsRefresh", PathArgs::Cluster(c)) => {
                self.describe_insights_refresh(&req, c)
            }
            ("StartInsightsRefresh", PathArgs::Cluster(c)) => self.start_insights_refresh(&req, c),
            ("AssociateEncryptionConfig", PathArgs::Cluster(c)) => {
                self.associate_encryption_config(&req, c)
            }
            ("CancelUpdate", PathArgs::Update { name, update_id }) => {
                self.cancel_update(&req, name, update_id)
            }
            ("RegisterCluster", _) => self.register_cluster(&req),
            ("DeregisterCluster", PathArgs::Name(n)) => self.deregister_cluster(&req, n),
            ("DescribeClusterVersions", _) => self.describe_cluster_versions(&req),
            ("CreateCapability", PathArgs::Cluster(c)) => self.create_capability(&req, c),
            ("ListCapabilities", PathArgs::Cluster(c)) => self.list_capabilities(&req, c),
            ("DescribeCapability", PathArgs::ClusterChild { cluster, name }) => {
                self.describe_capability(&req, cluster, name)
            }
            ("DeleteCapability", PathArgs::ClusterChild { cluster, name }) => {
                self.delete_capability(&req, cluster, name)
            }
            ("UpdateCapability", PathArgs::ClusterChild { cluster, name }) => {
                self.update_capability(&req, cluster, name)
            }
            ("CreateEksAnywhereSubscription", _) => self.create_eks_anywhere_subscription(&req),
            ("ListEksAnywhereSubscriptions", _) => self.list_eks_anywhere_subscriptions(&req),
            ("DescribeEksAnywhereSubscription", PathArgs::Name(id)) => {
                self.describe_eks_anywhere_subscription(&req, id)
            }
            ("DeleteEksAnywhereSubscription", PathArgs::Name(id)) => {
                self.delete_eks_anywhere_subscription(&req, id)
            }
            ("UpdateEksAnywhereSubscription", PathArgs::Name(id)) => {
                self.update_eks_anywhere_subscription(&req, id)
            }
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

fn not_found_access_entry(principal_arn: &str) -> impl Fn() -> AwsServiceError + 'static {
    let principal_arn = principal_arn.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No access entry found for principal ARN: {principal_arn}."),
        )
    }
}

fn not_found_identity_provider_config(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No identity provider config found for name: {name}."),
        )
    }
}

fn not_found_pod_identity_association(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No pod identity association found for id: {id}."),
        )
    }
}

/// Validate that a string is a well-formed EKS ARN, rejecting anything else
/// with a BadRequestException.
fn validate_eks_arn(arn: &str) -> Result<(), AwsServiceError> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[2] != "eks" {
        return Err(bad_request(format!("Invalid EKS ARN: {arn}")));
    }
    Ok(())
}

/// Which resource a tagging ARN points at. Located by scanning the stored
/// resources for a matching ARN (rather than reverse-parsing every ARN shape),
/// so the mutable borrow can be taken precisely afterwards.
enum TagTarget {
    Cluster(String),
    Nodegroup(String, String),
    Fargate(String, String),
    Addon(String, String),
    AccessEntry(String, String),
    Idp(String, String),
    PodIdentity(String, String),
    Capability(String, String),
    Subscription(String),
    /// An ARN that matched no tracked resource; tags are kept in the account's
    /// side `tags` map keyed by the ARN.
    Generic(String),
}

fn locate_tag_target(state: &crate::state::EksState, arn: &str) -> TagTarget {
    if let Some((n, _)) = state.clusters.iter().find(|(_, c)| c.arn == arn) {
        return TagTarget::Cluster(n.clone());
    }
    for (cn, m) in &state.nodegroups {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Nodegroup(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.fargate_profiles {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Fargate(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.addons {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Addon(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.access_entries {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::AccessEntry(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.identity_provider_configs {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Idp(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.pod_identity_associations {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.association_arn == arn) {
            return TagTarget::PodIdentity(cn.clone(), k.clone());
        }
    }
    for (cn, m) in &state.capabilities {
        if let Some((k, _)) = m.iter().find(|(_, r)| r.arn == arn) {
            return TagTarget::Capability(cn.clone(), k.clone());
        }
    }
    if let Some((k, _)) = state
        .eks_anywhere_subscriptions
        .iter()
        .find(|(_, r)| r.arn == arn)
    {
        return TagTarget::Subscription(k.clone());
    }
    TagTarget::Generic(arn.to_string())
}

fn tags_mut<'a>(
    state: &'a mut crate::state::EksState,
    target: &TagTarget,
) -> &'a mut crate::state::TagMap {
    // Every arm but `Generic` was just located under the same write lock, so the
    // lookups cannot miss.
    match target {
        TagTarget::Cluster(n) => &mut state.clusters.get_mut(n).unwrap().tags,
        TagTarget::Nodegroup(c, k) => {
            &mut state
                .nodegroups
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Fargate(c, k) => {
            &mut state
                .fargate_profiles
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Addon(c, k) => &mut state.addons.get_mut(c).unwrap().get_mut(k).unwrap().tags,
        TagTarget::AccessEntry(c, k) => {
            &mut state
                .access_entries
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Idp(c, k) => {
            &mut state
                .identity_provider_configs
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::PodIdentity(c, k) => {
            &mut state
                .pod_identity_associations
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Capability(c, k) => {
            &mut state
                .capabilities
                .get_mut(c)
                .unwrap()
                .get_mut(k)
                .unwrap()
                .tags
        }
        TagTarget::Subscription(k) => {
            &mut state.eks_anywhere_subscriptions.get_mut(k).unwrap().tags
        }
        TagTarget::Generic(a) => state.tags.entry(a.clone()).or_default(),
    }
}

fn tags_ref<'a>(
    state: &'a crate::state::EksState,
    target: &TagTarget,
) -> Option<&'a crate::state::TagMap> {
    match target {
        TagTarget::Cluster(n) => state.clusters.get(n).map(|c| &c.tags),
        TagTarget::Nodegroup(c, k) => state
            .nodegroups
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Fargate(c, k) => state
            .fargate_profiles
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Addon(c, k) => state.addons.get(c).and_then(|m| m.get(k)).map(|r| &r.tags),
        TagTarget::AccessEntry(c, k) => state
            .access_entries
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Idp(c, k) => state
            .identity_provider_configs
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::PodIdentity(c, k) => state
            .pod_identity_associations
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Capability(c, k) => state
            .capabilities
            .get(c)
            .and_then(|m| m.get(k))
            .map(|r| &r.tags),
        TagTarget::Subscription(k) => state.eks_anywhere_subscriptions.get(k).map(|r| &r.tags),
        TagTarget::Generic(a) => state.tags.get(a),
    }
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
        .unwrap_or("172.20.0.0/16")
        .to_string();
    // `elasticLoadBalancing` is always present in the response; `enabled`
    // defaults to false unless the caller opts in (auto mode / EKS Auto).
    let elb_enabled = req
        .and_then(|v| v.get("elasticLoadBalancing"))
        .and_then(|v| v.get("enabled"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    json!({
        "serviceIpv4Cidr": service_cidr,
        "ipFamily": ip_family,
        "elasticLoadBalancing": { "enabled": elb_enabled },
    })
}

/// Build an `AccessConfigResponse` object. The API only reports
/// `authenticationMode` (bootstrap-creator permission is a create-only input),
/// defaulting to `CONFIG_MAP` when the caller omits `accessConfig`.
fn build_access_config(req: Option<&Value>) -> Value {
    let mode = req
        .and_then(|v| v.get("authenticationMode"))
        .and_then(|v| v.as_str())
        .unwrap_or("CONFIG_MAP")
        .to_string();
    json!({ "authenticationMode": mode })
}

/// Build an `UpgradePolicyResponse` object, defaulting `supportType` to
/// `EXTENDED` (extended support enabled) when the caller omits `upgradePolicy`.
fn build_upgrade_policy(req: Option<&Value>) -> Value {
    let support = req
        .and_then(|v| v.get("supportType"))
        .and_then(|v| v.as_str())
        .unwrap_or("EXTENDED")
        .to_string();
    json!({ "supportType": support })
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
    let mut out = json!({
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
        "accessConfig": c.access_config,
        "upgradePolicy": c.upgrade_policy,
    });
    if let Some(cc) = &c.connector_config {
        out["connectorConfig"] = cc.clone();
    }
    if let Some(ec) = &c.encryption_config {
        out["encryptionConfig"] = ec.clone();
    }
    if let Some(v) = &c.compute_config {
        out["computeConfig"] = v.clone();
    }
    if let Some(v) = &c.storage_config {
        out["storageConfig"] = v.clone();
    }
    if let Some(v) = &c.zonal_shift_config {
        out["zonalShiftConfig"] = v.clone();
    }
    if let Some(v) = &c.remote_network_config {
        out["remoteNetworkConfig"] = v.clone();
    }
    if let Some(v) = &c.control_plane_scaling_config {
        out["controlPlaneScalingConfig"] = v.clone();
    }
    if let Some(v) = c.deletion_protection {
        out["deletionProtection"] = Value::Bool(v);
    }
    out
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

/// Split a principal ARN into its access-entry `(type, name)` ARN segments.
/// `arn:aws:iam::123:role/Foo` -> `("role", "Foo")`; users map to `user`;
/// anything else (root, assumed-role, unrecognised) falls back to `standard`.
fn principal_parts(principal_arn: &str) -> (String, String) {
    let resource = principal_arn
        .split(':')
        .next_back()
        .unwrap_or(principal_arn);
    let (kind, name) = resource.split_once('/').unwrap_or(("", resource));
    let name = name.rsplit('/').next().unwrap_or(name);
    let type_seg = match kind {
        "role" | "assumed-role" => "role",
        "user" => "user",
        _ => "standard",
    };
    (type_seg.to_string(), name.to_string())
}

/// The default `username` EKS derives for an access entry when the caller omits
/// one: `arn:aws:iam::123:role/Foo` -> a role SessionName template that mirrors
/// what AWS records for a role principal.
fn default_username(principal_arn: &str) -> String {
    let (type_seg, name) = principal_parts(principal_arn);
    match type_seg.as_str() {
        "role" => format!("arn:aws:sts::{{{{AccountID}}}}:assumed-role/{name}/{{{{SessionName}}}}"),
        _ => principal_arn.to_string(),
    }
}

/// Parse a `StringList` request member into a `Vec<String>`.
fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Build an `AccessScope` response object, defaulting `type` to `cluster` (the
/// AWS default) and echoing any supplied namespaces.
fn build_access_scope(req: Option<&Value>) -> Value {
    let scope_type = req
        .and_then(|v| v.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("cluster")
        .to_string();
    let namespaces = req
        .and_then(|v| v.get("namespaces"))
        .cloned()
        .unwrap_or_else(|| json!([]));
    json!({ "type": scope_type, "namespaces": namespaces })
}

fn access_entry_json(e: &AccessEntry) -> Value {
    json!({
        "clusterName": e.cluster_name,
        "principalArn": e.principal_arn,
        "kubernetesGroups": e.kubernetes_groups,
        "accessEntryArn": e.arn,
        "createdAt": timestamp_to_number(e.created_at),
        "modifiedAt": timestamp_to_number(e.modified_at),
        "tags": e.tags,
        "username": e.username,
        "type": e.type_,
    })
}

fn associated_policy_json(ap: &AssociatedPolicy) -> Value {
    json!({
        "policyArn": ap.policy_arn,
        "accessScope": ap.access_scope,
        "associatedAt": timestamp_to_number(ap.associated_at),
        "modifiedAt": timestamp_to_number(ap.modified_at),
    })
}

/// Read an optional string member from a JSON object.
fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn identity_provider_config_json(c: &IdentityProviderConfig) -> Value {
    let mut oidc = json!({
        "identityProviderConfigName": c.name,
        "identityProviderConfigArn": c.arn,
        "clusterName": c.cluster_name,
        "issuerUrl": c.issuer_url,
        "clientId": c.client_id,
        "requiredClaims": c.required_claims,
        "status": c.status,
        "tags": c.tags,
    });
    if let Some(v) = &c.username_claim {
        oidc["usernameClaim"] = Value::String(v.clone());
    }
    if let Some(v) = &c.username_prefix {
        oidc["usernamePrefix"] = Value::String(v.clone());
    }
    if let Some(v) = &c.groups_claim {
        oidc["groupsClaim"] = Value::String(v.clone());
    }
    if let Some(v) = &c.groups_prefix {
        oidc["groupsPrefix"] = Value::String(v.clone());
    }
    json!({ "oidc": oidc })
}

fn pod_identity_association_json(a: &PodIdentityAssociation) -> Value {
    let mut out = json!({
        "clusterName": a.cluster_name,
        "namespace": a.namespace,
        "serviceAccount": a.service_account,
        "roleArn": a.role_arn,
        "associationArn": a.association_arn,
        "associationId": a.association_id,
        "createdAt": timestamp_to_number(a.created_at),
        "modifiedAt": timestamp_to_number(a.modified_at),
        "disableSessionTags": a.disable_session_tags,
        "tags": a.tags,
    });
    if let Some(v) = &a.target_role_arn {
        out["targetRoleArn"] = Value::String(v.clone());
    }
    if let Some(v) = &a.external_id {
        out["externalId"] = Value::String(v.clone());
    }
    out
}

fn pod_identity_association_summary_json(a: &PodIdentityAssociation) -> Value {
    json!({
        "clusterName": a.cluster_name,
        "namespace": a.namespace,
        "serviceAccount": a.service_account,
        "associationArn": a.association_arn,
        "associationId": a.association_id,
    })
}

/// The real AWS EKS cluster access-policy catalogue returned by
/// `ListAccessPolicies`. Every entry is an `arn:aws:eks::aws:cluster-access-policy/*`
/// managed policy.
fn access_policy_catalog() -> Vec<Value> {
    const POLICIES: &[&str] = &[
        "AmazonEKSClusterAdminPolicy",
        "AmazonEKSAdminPolicy",
        "AmazonEKSEditPolicy",
        "AmazonEKSViewPolicy",
        "AmazonEKSAdminViewPolicy",
        "AmazonEKSAutoNodePolicy",
        "AmazonEKSBlockStoragePolicy",
        "AmazonEKSLoadBalancingPolicy",
        "AmazonEKSNetworkingPolicy",
        "AmazonEKSComputePolicy",
    ];
    POLICIES
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "arn": format!("arn:aws:eks::aws:cluster-access-policy/{name}"),
            })
        })
        .collect()
}

fn not_found_insight(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No insight found for id: {id}."),
        )
    }
}

fn not_found_capability(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No capability found for name: {name}."),
        )
    }
}

fn not_found_subscription(id: &str) -> impl Fn() -> AwsServiceError + 'static {
    let id = id.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("No EKS Anywhere subscription found for id: {id}."),
        )
    }
}

/// Render a connected (registered) cluster. Same shape as `cluster_json` but
/// tolerant of the empty fields a not-yet-activated connected cluster carries.
fn connected_cluster_json(c: &Cluster, id: &str) -> Value {
    cluster_json(c, id)
}

/// A set of plausible upgrade-readiness Insights EKS auto-generates for a
/// cluster. All seed as `PASSING` (a healthy cluster) so List/Describe return
/// real, non-empty content. Ids are generated per seed so Describe round-trips.
fn default_insights(cluster_version: &str) -> Vec<Insight> {
    let now = Utc::now();
    let mk = |name: &str, category: &str, description: &str, recommendation: &str| Insight {
        id: uuid::Uuid::new_v4().to_string(),
        name: name.to_string(),
        category: category.to_string(),
        kubernetes_version: cluster_version.to_string(),
        description: description.to_string(),
        recommendation: recommendation.to_string(),
        status: "PASSING".to_string(),
        reason: "No deprecated API usage detected.".to_string(),
        last_refresh_time: now,
        last_transition_time: now,
    };
    vec![
        mk(
            "Deprecated APIs removed in Kubernetes",
            "UPGRADE_READINESS",
            "Checks for usage of Kubernetes APIs that are removed in the next version.",
            "Migrate any deprecated API usage to the supported apiVersion before upgrading.",
        ),
        mk(
            "Kubelet version skew",
            "UPGRADE_READINESS",
            "Checks that node kubelet versions are within the supported skew of the control plane.",
            "Upgrade node groups so kubelet stays within two minor versions of the control plane.",
        ),
        mk(
            "EKS add-on version compatibility",
            "UPGRADE_READINESS",
            "Checks that installed EKS add-on versions are compatible with the target cluster version.",
            "Update add-ons to a version compatible with the target Kubernetes version.",
        ),
    ]
}

fn insight_status_json(i: &Insight) -> Value {
    json!({ "status": i.status, "reason": i.reason })
}

fn insight_json(i: &Insight) -> Value {
    json!({
        "id": i.id,
        "name": i.name,
        "category": i.category,
        "kubernetesVersion": i.kubernetes_version,
        "lastRefreshTime": timestamp_to_number(i.last_refresh_time),
        "lastTransitionTime": timestamp_to_number(i.last_transition_time),
        "description": i.description,
        "insightStatus": insight_status_json(i),
        "recommendation": i.recommendation,
        "additionalInfo": {},
        "resources": [],
    })
}

fn insight_summary_json(i: &Insight) -> Value {
    json!({
        "id": i.id,
        "name": i.name,
        "category": i.category,
        "kubernetesVersion": i.kubernetes_version,
        "lastRefreshTime": timestamp_to_number(i.last_refresh_time),
        "lastTransitionTime": timestamp_to_number(i.last_transition_time),
        "description": i.description,
        "insightStatus": insight_status_json(i),
    })
}

fn insights_refresh_json(r: &InsightsRefresh) -> Value {
    let mut out = json!({
        "message": "Insights refresh for the cluster.",
        "status": r.status,
        "startedAt": timestamp_to_number(r.started_at),
    });
    if let Some(ended) = r.ended_at {
        out["endedAt"] = timestamp_to_number(ended);
    }
    out
}

/// A real catalogue of recent Kubernetes minor versions with plausible
/// platform versions, release dates, and support windows, returned by
/// `DescribeClusterVersions`. 1.31 is the default.
fn cluster_version_catalog(cluster_type: &str) -> Vec<Value> {
    // (version, patch, platformVersion, releaseYmd, eoStandardYmd, eoExtendedYmd,
    //  status, default)
    type VersionRow = (
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
        bool,
    );
    const ROWS: &[VersionRow] = &[
        (
            "1.28",
            "1.28.15",
            "eks.30",
            "2023-09-26",
            "2024-11-26",
            "2025-11-26",
            "extended_support",
            false,
        ),
        (
            "1.29",
            "1.29.10",
            "eks.24",
            "2024-01-23",
            "2025-03-23",
            "2026-03-23",
            "extended_support",
            false,
        ),
        (
            "1.30",
            "1.30.6",
            "eks.20",
            "2024-05-23",
            "2025-07-23",
            "2026-07-23",
            "standard_support",
            false,
        ),
        (
            "1.31",
            "1.31.2",
            "eks.9",
            "2024-09-26",
            "2025-11-26",
            "2026-11-26",
            "standard_support",
            true,
        ),
        (
            "1.32",
            "1.32.0",
            "eks.2",
            "2025-01-23",
            "2026-03-23",
            "2027-03-23",
            "standard_support",
            false,
        ),
    ];
    ROWS.iter()
        .map(
            |(ver, patch, plat, release, eos, eoe, status, is_default)| {
                let version_status = match *status {
                    "standard_support" => "STANDARD_SUPPORT",
                    "extended_support" => "EXTENDED_SUPPORT",
                    _ => "UNSUPPORTED",
                };
                json!({
                    "clusterVersion": ver,
                    "clusterType": cluster_type,
                    "defaultPlatformVersion": plat,
                    "defaultVersion": is_default,
                    "releaseDate": date_to_number(release),
                    "endOfStandardSupportDate": date_to_number(eos),
                    "endOfExtendedSupportDate": date_to_number(eoe),
                    "status": status,
                    "versionStatus": version_status,
                    "kubernetesPatchVersion": patch,
                })
            },
        )
        .collect()
}

/// Parse a `YYYY-MM-DD` date into an epoch-seconds JSON number (midnight UTC).
fn date_to_number(ymd: &str) -> Value {
    let ts = chrono::NaiveDate::parse_from_str(ymd, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc())
        .unwrap_or_else(Utc::now);
    timestamp_to_number(ts)
}

/// Keep only the `argoCd` member of a capability configuration (the sole
/// member of `CapabilityConfigurationResponse`), so the echoed config stays
/// shape-faithful. Returns `None` when nothing recognisable is present.
fn normalize_capability_configuration(req: Option<&Value>) -> Option<Value> {
    let argo = req.and_then(|v| v.get("argoCd"))?;
    Some(json!({ "argoCd": argo }))
}

fn capability_json(c: &Capability) -> Value {
    let mut out = json!({
        "capabilityName": c.name,
        "arn": c.arn,
        "clusterName": c.cluster_name,
        "type": c.type_,
        "roleArn": c.role_arn,
        "status": c.status,
        "version": c.version,
        "tags": c.tags,
        "health": { "issues": [] },
        "createdAt": timestamp_to_number(c.created_at),
        "modifiedAt": timestamp_to_number(c.modified_at),
    });
    if let Some(cfg) = &c.configuration {
        out["configuration"] = cfg.clone();
    }
    if let Some(p) = &c.delete_propagation_policy {
        out["deletePropagationPolicy"] = Value::String(p.clone());
    }
    out
}

fn capability_summary_json(c: &Capability) -> Value {
    json!({
        "capabilityName": c.name,
        "arn": c.arn,
        "type": c.type_,
        "status": c.status,
        "version": c.version,
        "createdAt": timestamp_to_number(c.created_at),
        "modifiedAt": timestamp_to_number(c.modified_at),
    })
}

fn subscription_json(s: &EksAnywhereSubscription) -> Value {
    json!({
        "id": s.id,
        "arn": s.arn,
        "createdAt": timestamp_to_number(s.created_at),
        "effectiveDate": timestamp_to_number(s.effective_date),
        "expirationDate": timestamp_to_number(s.expiration_date),
        "licenseQuantity": s.license_quantity,
        "licenseType": s.license_type,
        "term": { "duration": s.term_duration, "unit": s.term_unit },
        "status": s.status,
        "autoRenew": s.auto_renew,
        "licenseArns": [],
        "licenses": [],
        "tags": s.tags,
    })
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
    async fn cluster_reports_access_config_upgrade_policy_and_elb_defaults() {
        let svc = EksService::new(make_state());
        // Create with no accessConfig/upgradePolicy/kubernetesNetworkConfig: the
        // response defaults them the way the real control plane does.
        let resp = svc
            .handle(make_request(Method::POST, "/clusters", &create_body("c1")))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let c = &v["cluster"];
        assert_eq!(c["accessConfig"]["authenticationMode"], "CONFIG_MAP");
        assert_eq!(c["upgradePolicy"]["supportType"], "EXTENDED");
        assert_eq!(
            c["kubernetesNetworkConfig"]["elasticLoadBalancing"]["enabled"],
            false
        );
        assert_eq!(
            c["kubernetesNetworkConfig"]["serviceIpv4Cidr"],
            "172.20.0.0/16"
        );
        assert_eq!(c["kubernetesNetworkConfig"]["ipFamily"], "ipv4");

        // Describe echoes the same shape (drift-free round-trip).
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            v["cluster"]["accessConfig"]["authenticationMode"],
            "CONFIG_MAP"
        );
        assert_eq!(v["cluster"]["upgradePolicy"]["supportType"], "EXTENDED");
        assert_eq!(
            v["cluster"]["kubernetesNetworkConfig"]["elasticLoadBalancing"]["enabled"],
            false
        );
    }

    #[tokio::test]
    async fn cluster_echoes_supplied_access_config() {
        let svc = EksService::new(make_state());
        let body = json!({
            "name": "c1",
            "roleArn": "arn:aws:iam::111122223333:role/eks-cluster",
            "resourcesVpcConfig": { "subnetIds": ["subnet-1", "subnet-2"] },
            "accessConfig": { "authenticationMode": "API" },
        })
        .to_string();
        let resp = svc
            .handle(make_request(Method::POST, "/clusters", &body))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["accessConfig"]["authenticationMode"], "API");
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

    #[tokio::test]
    async fn update_cluster_config_persists_access_and_upgrade() {
        let svc = EksService::new(make_state());
        svc.handle(make_request(Method::POST, "/clusters", &create_body("uc")))
            .await
            .unwrap();
        // Settle to ACTIVE.
        svc.handle(make_request(Method::GET, "/clusters/uc", ""))
            .await
            .unwrap();
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/uc/update-config",
                &json!({
                    "accessConfig": { "authenticationMode": "API_AND_CONFIG_MAP" },
                    "upgradePolicy": { "supportType": "STANDARD" },
                    "computeConfig": { "enabled": true }
                })
                .to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["status"], "InProgress");

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/uc", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            v["cluster"]["accessConfig"]["authenticationMode"],
            "API_AND_CONFIG_MAP"
        );
        assert_eq!(v["cluster"]["upgradePolicy"]["supportType"], "STANDARD");
        assert_eq!(v["cluster"]["computeConfig"]["enabled"], true);
    }

    #[tokio::test]
    async fn tag_nodegroup_sub_resource_arn() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "tc").await;
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/tc/node-groups",
                &nodegroup_body("tng"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let ng_arn = v["nodegroup"]["nodegroupArn"].as_str().unwrap().to_string();
        let encoded = ng_arn.replace('/', "%2F").replace(':', "%3A");

        // Tagging a node-group ARN must succeed (previously rejected with 400).
        svc.handle(make_request(
            Method::POST,
            &format!("/tags/{encoded}"),
            &json!({ "tags": { "team": "core" } }).to_string(),
        ))
        .await
        .unwrap();

        let resp = svc
            .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["tags"]["team"], "core");

        // And it is reflected on the node group itself.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/tc/node-groups/tng",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroup"]["tags"]["team"], "core");
    }

    #[tokio::test]
    async fn delete_cluster_blocked_by_live_nodegroup() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "bc").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/bc/node-groups",
            &nodegroup_body("ng"),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(Method::DELETE, "/clusters/bc", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    #[tokio::test]
    async fn delete_cluster_cascades_subresources() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "cc").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/cc/node-groups",
            &nodegroup_body("ng"),
        ))
        .await
        .unwrap();
        svc.handle(make_request(
            Method::DELETE,
            "/clusters/cc/node-groups/ng",
            "",
        ))
        .await
        .unwrap();
        svc.handle(make_request(Method::DELETE, "/clusters/cc", ""))
            .await
            .unwrap();
        // Recreate the same name: no orphaned node groups may leak through.
        create_cluster(&svc, "cc").await;
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/cc/node-groups", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["nodegroups"], json!([]));
    }

    #[tokio::test]
    async fn describe_cluster_versions_filters_by_version_status() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/cluster-versions?versionStatus=EXTENDED_SUPPORT",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let rows = v["clusterVersions"].as_array().unwrap();
        assert!(!rows.is_empty());
        assert!(rows
            .iter()
            .all(|r| r["versionStatus"] == "EXTENDED_SUPPORT"));
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
        // An unknown sub-resource collection must not accidentally match via
        // the node-group/fargate-profile/addon/access-entry/idp/pod-identity
        // arms; the router falls through to UnknownOperationException.
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        let err = svc
            .handle(make_request(Method::GET, "/clusters/c1/insights", ""))
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

    // -----------------------------------------------------------------------
    // Access entries / access policies
    // -----------------------------------------------------------------------

    const PRINCIPAL: &str = "arn:aws:iam::111122223333:role/dev";

    fn access_entry_body() -> String {
        json!({
            "principalArn": PRINCIPAL,
            "kubernetesGroups": ["viewers"],
            "tags": { "team": "core" }
        })
        .to_string()
    }

    fn url_encode(s: &str) -> String {
        s.replace('%', "%25")
            .replace(':', "%3A")
            .replace('/', "%2F")
    }

    fn encoded_principal() -> String {
        url_encode(PRINCIPAL)
    }

    #[tokio::test]
    async fn access_entry_full_lifecycle() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        // Create.
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/access-entries",
                &access_entry_body(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["accessEntry"]["principalArn"], PRINCIPAL);
        assert_eq!(v["accessEntry"]["clusterName"], "c1");
        assert_eq!(v["accessEntry"]["type"], "STANDARD");
        assert_eq!(v["accessEntry"]["kubernetesGroups"], json!(["viewers"]));
        assert_eq!(v["accessEntry"]["tags"]["team"], "core");
        assert!(v["accessEntry"]["accessEntryArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks:us-east-1:111122223333:access-entry/c1/role/"));
        assert!(v["accessEntry"]["username"]
            .as_str()
            .unwrap()
            .contains("dev"));

        let enc = encoded_principal();

        // Describe.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/access-entries/{enc}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["accessEntry"]["principalArn"], PRINCIPAL);

        // List.
        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1/access-entries", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["accessEntries"], json!([PRINCIPAL]));

        // Update.
        let resp = svc
            .handle(make_request(
                Method::POST,
                &format!("/clusters/c1/access-entries/{enc}"),
                &json!({ "kubernetesGroups": ["admins"], "username": "custom" }).to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["accessEntry"]["kubernetesGroups"], json!(["admins"]));
        assert_eq!(v["accessEntry"]["username"], "custom");

        // Associate an access policy.
        let policy = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy";
        let resp = svc
            .handle(make_request(
                Method::POST,
                &format!("/clusters/c1/access-entries/{enc}/access-policies"),
                &json!({
                    "policyArn": policy,
                    "accessScope": { "type": "namespace", "namespaces": ["default"] }
                })
                .to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["clusterName"], "c1");
        assert_eq!(v["principalArn"], PRINCIPAL);
        assert_eq!(v["associatedAccessPolicy"]["policyArn"], policy);
        assert_eq!(
            v["associatedAccessPolicy"]["accessScope"]["type"],
            "namespace"
        );

        // List associated policies.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/access-entries/{enc}/access-policies"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["clusterName"], "c1");
        assert_eq!(v["associatedAccessPolicies"].as_array().unwrap().len(), 1);
        assert_eq!(v["associatedAccessPolicies"][0]["policyArn"], policy);

        // ListAccessEntries with associatedPolicyArn filter finds the entry.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/access-entries?associatedPolicyArn={policy}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["accessEntries"], json!([PRINCIPAL]));

        // Disassociate.
        let enc_policy = encoded_principal_of(policy);
        let resp = svc
            .handle(make_request(
                Method::DELETE,
                &format!("/clusters/c1/access-entries/{enc}/access-policies/{enc_policy}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v, json!({}));

        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/access-entries/{enc}/access-policies"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["associatedAccessPolicies"].as_array().unwrap().len(), 0);

        // Delete the entry.
        let resp = svc
            .handle(make_request(
                Method::DELETE,
                &format!("/clusters/c1/access-entries/{enc}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v, json!({}));

        let err = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/access-entries/{enc}"),
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    fn encoded_principal_of(s: &str) -> String {
        url_encode(s)
    }

    #[tokio::test]
    async fn access_entry_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/access-entries",
                &access_entry_body(),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn access_entry_duplicate_is_in_use() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/access-entries",
            &access_entry_body(),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/access-entries",
                &access_entry_body(),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    #[tokio::test]
    async fn describe_access_entry_missing_is_not_found() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        let err = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/access-entries/{}", encoded_principal()),
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn list_access_policies_catalog_is_non_empty() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(Method::GET, "/access-policies", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let policies = v["accessPolicies"].as_array().unwrap();
        assert!(policies.len() >= 10);
        let names: Vec<&str> = policies
            .iter()
            .map(|p| p["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"AmazonEKSClusterAdminPolicy"));
        assert!(names.contains(&"AmazonEKSViewPolicy"));
        assert!(policies[0]["arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks::aws:cluster-access-policy/"));
    }

    // -----------------------------------------------------------------------
    // Identity-provider configs
    // -----------------------------------------------------------------------

    fn idp_body(name: &str) -> String {
        json!({
            "oidc": {
                "identityProviderConfigName": name,
                "issuerUrl": "https://example.com",
                "clientId": "kubernetes",
                "usernameClaim": "email",
                "groupsClaim": "groups"
            },
            "tags": { "team": "core" }
        })
        .to_string()
    }

    #[tokio::test]
    async fn idp_associate_describe_list_disassociate() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        // Associate mints a tracked cluster-scoped Update and echoes tags.
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/identity-provider-configs/associate",
                &idp_body("oidc1"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "AssociateIdentityProviderConfig");
        assert_eq!(v["update"]["status"], "InProgress");
        assert_eq!(v["tags"]["team"], "core");
        let update_id = v["update"]["id"].as_str().unwrap().to_string();

        // The Update is cluster-scoped and settles on DescribeUpdate.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/updates/{update_id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["status"], "Successful");

        // Describe returns the OIDC config and settles CREATING -> ACTIVE.
        let describe =
            json!({ "identityProviderConfig": { "type": "oidc", "name": "oidc1" } }).to_string();
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/identity-provider-configs/describe",
                &describe,
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let oidc = &v["identityProviderConfig"]["oidc"];
        assert_eq!(oidc["identityProviderConfigName"], "oidc1");
        assert_eq!(oidc["issuerUrl"], "https://example.com");
        assert_eq!(oidc["clientId"], "kubernetes");
        assert_eq!(oidc["usernameClaim"], "email");
        assert_eq!(oidc["status"], "ACTIVE");
        assert!(oidc["identityProviderConfigArn"]
            .as_str()
            .unwrap()
            .starts_with(
                "arn:aws:eks:us-east-1:111122223333:identityproviderconfig/c1/oidc/oidc1/"
            ));

        // List returns the {type, name} summary.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/identity-provider-configs",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            v["identityProviderConfigs"],
            json!([{ "type": "oidc", "name": "oidc1" }])
        );

        // Disassociate mints another tracked Update and removes the config.
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/identity-provider-configs/disassociate",
                &describe,
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "DisassociateIdentityProviderConfig");

        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/identity-provider-configs",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["identityProviderConfigs"], json!([]));

        // Describe of the removed config is not found.
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/identity-provider-configs/describe",
                &describe,
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn idp_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/identity-provider-configs/associate",
                &idp_body("oidc1"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn idp_duplicate_is_in_use() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/identity-provider-configs/associate",
            &idp_body("oidc1"),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/identity-provider-configs/associate",
                &idp_body("oidc1"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    // -----------------------------------------------------------------------
    // Pod identity associations
    // -----------------------------------------------------------------------

    fn pod_identity_body(namespace: &str, sa: &str) -> String {
        json!({
            "namespace": namespace,
            "serviceAccount": sa,
            "roleArn": "arn:aws:iam::111122223333:role/pod-role",
            "tags": { "team": "core" }
        })
        .to_string()
    }

    #[tokio::test]
    async fn pod_identity_create_describe_list_update_delete() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/pod-identity-associations",
                &pod_identity_body("default", "app"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let assoc = &v["association"];
        assert_eq!(assoc["clusterName"], "c1");
        assert_eq!(assoc["namespace"], "default");
        assert_eq!(assoc["serviceAccount"], "app");
        assert_eq!(assoc["roleArn"], "arn:aws:iam::111122223333:role/pod-role");
        assert_eq!(assoc["disableSessionTags"], false);
        assert_eq!(assoc["tags"]["team"], "core");
        // No targetRoleArn was supplied, so no externalId is returned.
        assert!(assoc.get("externalId").is_none());
        let association_id = assoc["associationId"].as_str().unwrap().to_string();
        assert!(association_id.starts_with("a-"));
        assert!(assoc["associationArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks:us-east-1:111122223333:podidentityassociation/c1/a-"));

        // Describe round-trips.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/pod-identity-associations/{association_id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["association"]["serviceAccount"], "app");

        // List returns a summary, filterable by namespace.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/pod-identity-associations?namespace=default",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["associations"].as_array().unwrap().len(), 1);
        assert_eq!(v["associations"][0]["associationId"], association_id);

        // A non-matching namespace filter returns nothing.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/pod-identity-associations?namespace=other",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["associations"], json!([]));

        // Update upserts the role and cross-account target (minting externalId).
        let resp = svc
            .handle(make_request(
                Method::POST,
                &format!("/clusters/c1/pod-identity-associations/{association_id}"),
                &json!({
                    "roleArn": "arn:aws:iam::111122223333:role/new-role",
                    "targetRoleArn": "arn:aws:iam::444455556666:role/target",
                    "disableSessionTags": true
                })
                .to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            v["association"]["roleArn"],
            "arn:aws:iam::111122223333:role/new-role"
        );
        assert_eq!(
            v["association"]["targetRoleArn"],
            "arn:aws:iam::444455556666:role/target"
        );
        assert_eq!(v["association"]["disableSessionTags"], true);
        assert!(v["association"]["externalId"].is_string());

        // Delete returns the association and then it's gone.
        let resp = svc
            .handle(make_request(
                Method::DELETE,
                &format!("/clusters/c1/pod-identity-associations/{association_id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["association"]["associationId"], association_id);

        let err = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/pod-identity-associations/{association_id}"),
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn pod_identity_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/pod-identity-associations",
                &pod_identity_body("default", "app"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn pod_identity_duplicate_is_in_use() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/pod-identity-associations",
            &pod_identity_body("default", "app"),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/pod-identity-associations",
                &pod_identity_body("default", "app"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    // -----------------------------------------------------------------------
    // Insights
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn insights_list_describe_and_refresh() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        // ListInsights seeds and returns a non-empty, real catalogue.
        let resp = svc
            .handle(make_request(Method::POST, "/clusters/c1/insights", "{}"))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let insights = v["insights"].as_array().unwrap();
        assert!(!insights.is_empty());
        assert_eq!(insights[0]["insightStatus"]["status"], "PASSING");
        let id = insights[0]["id"].as_str().unwrap().to_string();

        // DescribeInsight round-trips by id.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/clusters/c1/insights/{id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["insight"]["id"], id);
        assert_eq!(v["insight"]["category"], "UPGRADE_READINESS");

        // StartInsightsRefresh returns only message + status.
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/insights-refresh",
                "{}",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["status"], "IN_PROGRESS");
        assert!(v.get("startedAt").is_none());

        // DescribeInsightsRefresh settles IN_PROGRESS -> COMPLETED.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/insights-refresh",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["status"], "COMPLETED");
        assert!(v.get("endedAt").is_some());
    }

    #[tokio::test]
    async fn insights_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(Method::POST, "/clusters/ghost/insights", "{}"))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn describe_insight_missing_is_not_found() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        let err = svc
            .handle(make_request(Method::GET, "/clusters/c1/insights/ghost", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    // -----------------------------------------------------------------------
    // Encryption config / cancel update / register / cluster versions
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn associate_encryption_config_mints_update() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        let body = json!({
            "encryptionConfig": [{
                "resources": ["secrets"],
                "provider": { "keyArn": "arn:aws:kms:us-east-1:111122223333:key/abc" }
            }]
        })
        .to_string();
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/encryption-config/associate",
                &body,
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "AssociateEncryptionConfig");
        assert_eq!(v["update"]["status"], "InProgress");
        let update_id = v["update"]["id"].as_str().unwrap().to_string();

        // The update is a cluster update discoverable via DescribeUpdate, and
        // CancelUpdate marks it Cancelled.
        let resp = svc
            .handle(make_request(
                Method::POST,
                &format!("/clusters/c1/updates/{update_id}/cancel-update"),
                "{}",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["status"], "Cancelled");
    }

    #[tokio::test]
    async fn encryption_config_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/encryption-config/associate",
                &json!({ "encryptionConfig": [] }).to_string(),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn register_and_deregister_cluster() {
        let svc = EksService::new(make_state());
        let body = json!({
            "name": "connected1",
            "connectorConfig": {
                "roleArn": "arn:aws:iam::111122223333:role/eks-connector",
                "provider": "EKS_ANYWHERE"
            }
        })
        .to_string();
        let resp = svc
            .handle(make_request(Method::POST, "/cluster-registrations", &body))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["name"], "connected1");
        assert_eq!(v["cluster"]["status"], "PENDING");
        assert_eq!(v["cluster"]["connectorConfig"]["provider"], "EKS_ANYWHERE");
        assert!(v["cluster"]["connectorConfig"]["activationId"].is_string());

        // Duplicate registration conflicts.
        let err = svc
            .handle(make_request(Method::POST, "/cluster-registrations", &body))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceInUseException");

        // Deregister removes the connected cluster.
        let resp = svc
            .handle(make_request(
                Method::DELETE,
                "/cluster-registrations/connected1",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["cluster"]["status"], "DELETING");

        let err = svc
            .handle(make_request(
                Method::DELETE,
                "/cluster-registrations/connected1",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn deregister_non_connected_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "regular").await;
        // A regular (non-registered) cluster can't be deregistered.
        let err = svc
            .handle(make_request(
                Method::DELETE,
                "/cluster-registrations/regular",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn describe_cluster_versions_catalog_is_non_empty() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(Method::GET, "/cluster-versions", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let versions = v["clusterVersions"].as_array().unwrap();
        assert!(versions.len() >= 5);
        let names: Vec<&str> = versions
            .iter()
            .map(|x| x["clusterVersion"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"1.31"));
        // Exactly one default version.
        assert_eq!(
            versions
                .iter()
                .filter(|x| x["defaultVersion"] == true)
                .count(),
            1
        );
        // AWS reports the default version first (index 0).
        assert_eq!(versions[0]["defaultVersion"], true);

        // defaultOnly filter narrows to the single default.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/cluster-versions?defaultOnly=true",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["clusterVersions"].as_array().unwrap().len(), 1);
        assert_eq!(v["clusterVersions"][0]["clusterVersion"], "1.31");
    }

    #[tokio::test]
    async fn describe_cluster_versions_rejects_bad_maxresults() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::GET,
                "/cluster-versions?maxResults=0",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.code(), "InvalidParameterException");
    }

    // -----------------------------------------------------------------------
    // Capabilities
    // -----------------------------------------------------------------------

    fn capability_body(name: &str) -> String {
        json!({
            "capabilityName": name,
            "type": "ACK",
            "roleArn": "arn:aws:iam::111122223333:role/eks-capability",
            "deletePropagationPolicy": "RETAIN",
            "tags": { "team": "core" }
        })
        .to_string()
    }

    #[tokio::test]
    async fn capability_create_describe_list_update_delete() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;

        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/capabilities",
                &capability_body("ack"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["capability"]["capabilityName"], "ack");
        assert_eq!(v["capability"]["status"], "CREATING");
        assert_eq!(v["capability"]["type"], "ACK");
        assert!(v["capability"]["arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks:us-east-1:111122223333:capability/c1/ack/"));

        // Describe settles CREATING -> ACTIVE.
        let resp = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/capabilities/ack",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["capability"]["status"], "ACTIVE");
        assert_eq!(v["capability"]["tags"]["team"], "core");

        let resp = svc
            .handle(make_request(Method::GET, "/clusters/c1/capabilities", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["capabilities"].as_array().unwrap().len(), 1);
        assert_eq!(v["capabilities"][0]["capabilityName"], "ack");

        // UpdateCapability mints a tracked cluster Update.
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/capabilities/ack",
                &json!({ "roleArn": "arn:aws:iam::111122223333:role/new" }).to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["update"]["type"], "CapabilityUpdate");

        // Delete.
        let resp = svc
            .handle(make_request(
                Method::DELETE,
                "/clusters/c1/capabilities/ack",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["capability"]["status"], "DELETING");

        let err = svc
            .handle(make_request(
                Method::GET,
                "/clusters/c1/capabilities/ack",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn capability_on_missing_cluster_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/ghost/capabilities",
                &capability_body("ack"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn capability_duplicate_is_in_use() {
        let svc = EksService::new(make_state());
        create_cluster(&svc, "c1").await;
        svc.handle(make_request(
            Method::POST,
            "/clusters/c1/capabilities",
            &capability_body("ack"),
        ))
        .await
        .unwrap();
        let err = svc
            .handle(make_request(
                Method::POST,
                "/clusters/c1/capabilities",
                &capability_body("ack"),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ResourceInUseException");
    }

    // -----------------------------------------------------------------------
    // EKS Anywhere subscriptions
    // -----------------------------------------------------------------------

    fn subscription_body(name: &str) -> String {
        json!({
            "name": name,
            "term": { "duration": 12, "unit": "MONTHS" },
            "licenseQuantity": 5,
            "licenseType": "Cluster",
            "autoRenew": false,
            "tags": { "team": "core" }
        })
        .to_string()
    }

    #[tokio::test]
    async fn subscription_create_describe_list_update_delete() {
        let svc = EksService::new(make_state());

        let resp = svc
            .handle(make_request(
                Method::POST,
                "/eks-anywhere-subscriptions",
                &subscription_body("sub1"),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let sub = &v["subscription"];
        assert_eq!(sub["status"], "ACTIVE");
        assert_eq!(sub["licenseQuantity"], 5);
        assert_eq!(sub["term"]["duration"], 12);
        assert_eq!(sub["autoRenew"], false);
        let id = sub["id"].as_str().unwrap().to_string();
        assert!(sub["arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:eks:us-east-1:111122223333:eks-anywhere-subscription/"));

        // Describe round-trips.
        let resp = svc
            .handle(make_request(
                Method::GET,
                &format!("/eks-anywhere-subscriptions/{id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["subscription"]["id"], id);

        // List returns the subscription.
        let resp = svc
            .handle(make_request(Method::GET, "/eks-anywhere-subscriptions", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["subscriptions"].as_array().unwrap().len(), 1);

        // Update toggles autoRenew.
        let resp = svc
            .handle(make_request(
                Method::POST,
                &format!("/eks-anywhere-subscriptions/{id}"),
                &json!({ "autoRenew": true }).to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["subscription"]["autoRenew"], true);

        // Delete.
        let resp = svc
            .handle(make_request(
                Method::DELETE,
                &format!("/eks-anywhere-subscriptions/{id}"),
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["subscription"]["status"], "DELETING");

        let err = svc
            .handle(make_request(
                Method::GET,
                &format!("/eks-anywhere-subscriptions/{id}"),
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn subscription_describe_missing_is_not_found() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::GET,
                "/eks-anywhere-subscriptions/ghost",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn subscription_rejects_bad_name() {
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/eks-anywhere-subscriptions",
                &json!({ "name": "-bad", "term": { "duration": 1, "unit": "MONTHS" } }).to_string(),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
    }

    #[tokio::test]
    async fn subscription_rejects_overflow_duration() {
        // Regression: a hostile `term.duration` used to overflow the
        // `DateTime + Duration` expiration arithmetic and panic the handler.
        let svc = EksService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::POST,
                "/eks-anywhere-subscriptions",
                &json!({ "name": "sub", "term": { "duration": 100_000_000_i64, "unit": "MONTHS" } })
                    .to_string(),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.code(), "InvalidParameterException");
    }

    #[tokio::test]
    async fn subscription_accepts_valid_duration() {
        let svc = EksService::new(make_state());
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/eks-anywhere-subscriptions",
                &json!({ "name": "sub-ok", "term": { "duration": 36, "unit": "MONTHS" } })
                    .to_string(),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["subscription"]["term"]["duration"], 36);
        assert_eq!(v["subscription"]["status"], "ACTIVE");
    }
}
