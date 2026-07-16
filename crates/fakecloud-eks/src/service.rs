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
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::eks_helpers::*;

use crate::state::{
    access_entry_arn, addon_arn, capability_arn, cluster_arn, eks_anywhere_subscription_arn,
    fargate_profile_arn, identity_provider_config_arn, nodegroup_arn, pod_identity_association_arn,
    AccessEntry, Addon, AssociatedPolicy, Capability, Cluster, EksAnywhereSubscription,
    EksSnapshot, FargateProfile, IdentityProviderConfig, InsightsRefresh, Nodegroup,
    PodIdentityAssociation, SharedEksState, DEFAULT_K8S_VERSION, EKS_SNAPSHOT_SCHEMA_VERSION,
};

/// The set of control-plane log types EKS reports on every cluster.
pub(crate) const LOG_TYPES: &[&str] = &[
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
            encryption_config: body
                .get("encryptionConfig")
                .filter(|v| v.is_array())
                .cloned(),
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
            if !ng.labels.is_object() {
                ng.labels = json!({});
            }
            if let Some(map) = ng.labels.as_object_mut() {
                if let Some(add) = labels.get("addOrUpdateLabels").and_then(|v| v.as_object()) {
                    for (k, v) in add {
                        map.insert(k.clone(), v.clone());
                    }
                }
                if let Some(remove) = labels.get("removeLabels").and_then(|v| v.as_array()) {
                    for k in remove.iter().filter_map(|v| v.as_str()) {
                        map.remove(k);
                    }
                }
            }
            params.push(("LabelsToAdd".to_string(), labels.to_string()));
        }
        if let Some(taints) = body.get("taints") {
            if !ng.taints.is_array() {
                ng.taints = json!([]);
            }
            if let Some(arr) = ng.taints.as_array_mut() {
                // Remove first so an add+remove of the same key in one call keeps the add.
                if let Some(remove) = taints.get("removeTaints").and_then(|v| v.as_array()) {
                    for rt in remove {
                        let id = taint_identity(rt);
                        arr.retain(|t| taint_identity(t) != id);
                    }
                }
                if let Some(add) = taints.get("addOrUpdateTaints").and_then(|v| v.as_array()) {
                    for nt in add {
                        let id = taint_identity(nt);
                        if let Some(existing) = arr.iter_mut().find(|t| taint_identity(t) == id) {
                            *existing = nt.clone();
                        } else {
                            arr.push(nt.clone());
                        }
                    }
                }
            }
            params.push(("Taints".to_string(), taints.to_string()));
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

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
