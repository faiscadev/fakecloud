//! `AWS::EKS::*` CloudFormation provisioning. Builds the EKS crate's OWN
//! state structs (clusters, node groups, Fargate profiles, add-ons, access
//! entries, OIDC identity-provider configs, and Pod Identity associations) and
//! inserts them into the shared EKS state under the provisioner's
//! account/region, so a CFN-provisioned resource reads back through
//! `DescribeCluster` / `DescribeNodegroup` / etc. exactly like an API-created
//! one. The EKS service's snapshot hook (registered in the server) then
//! persists them so they survive a restart (#1766 lesson).

use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_eks::{
    access_entry_arn, addon_arn, cluster_arn, fargate_profile_arn, identity_provider_config_arn,
    nodegroup_arn, pod_identity_association_arn, AccessEntry, Addon, Cluster, FargateProfile,
    IdentityProviderConfig, Nodegroup, PodIdentityAssociation, TagMap, DEFAULT_K8S_VERSION,
};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

/// Parse an EKS `Tags` property. EKS uses a JSON *map* (`{"k":"v"}`) for Tags in
/// CloudFormation, not the `[{Key,Value}]` list other services use.
fn parse_eks_tags(value: Option<&Value>) -> TagMap {
    let mut out = TagMap::new();
    if let Some(obj) = value.and_then(|v| v.as_object()) {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// Recover the synthetic cluster id (lowercase hex) from the stored endpoint —
/// the same derivation the direct EKS service uses so the OIDC issuer / `Id`
/// stay stable across describe and CFN GetAtt.
fn cluster_id_from_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("https://")
        .and_then(|s| s.split('.').next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default()
}

/// Short hex id (dash-stripped uuid, truncated to 17 chars) used for the
/// synthetic `sg-`/`vpc-` ids the direct service embeds in the VPC config.
fn short_id(id: &str) -> String {
    let stripped = id.replace('-', "");
    stripped[..17.min(stripped.len())].to_string()
}

impl ResourceProvisioner {
    // --- EKS Cluster ---

    pub(super) fn create_eks_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .ok_or("RoleArn is required")?
            .to_string();
        let vpc_req = props
            .get("ResourcesVpcConfig")
            .ok_or("ResourcesVpcConfig is required")?;
        let version = props
            .get("Version")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_K8S_VERSION)
            .to_string();

        let id = Uuid::new_v4().to_string();
        let sid = short_id(&id);
        let arn = cluster_arn(&self.region, &self.account_id, &name);
        let endpoint = format!(
            "https://{}.gr7.{}.eks.amazonaws.com",
            id.replace('-', "").to_uppercase(),
            self.region
        );
        // Build the VpcConfigResponse-shaped object echoed on describe, mapping
        // the CFN PascalCase members to the API's camelCase ones.
        let resources_vpc_config = json!({
            "subnetIds": vpc_req.get("SubnetIds").cloned().unwrap_or_else(|| json!([])),
            "securityGroupIds": vpc_req.get("SecurityGroupIds").cloned().unwrap_or_else(|| json!([])),
            "clusterSecurityGroupId": format!("sg-{sid}"),
            "vpcId": format!("vpc-{sid}"),
            "endpointPublicAccess": vpc_req
                .get("EndpointPublicAccess")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            "endpointPrivateAccess": vpc_req
                .get("EndpointPrivateAccess")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            "publicAccessCidrs": vpc_req
                .get("PublicAccessCidrs")
                .cloned()
                .unwrap_or_else(|| json!(["0.0.0.0/0"])),
        });
        let knc = props.get("KubernetesNetworkConfig");
        let kubernetes_network_config = json!({
            "serviceIpv4Cidr": knc
                .and_then(|v| v.get("ServiceIpv4Cidr"))
                .and_then(|v| v.as_str())
                .unwrap_or("172.20.0.0/16"),
            "ipFamily": knc
                .and_then(|v| v.get("IpFamily"))
                .and_then(|v| v.as_str())
                .unwrap_or("ipv4"),
            "elasticLoadBalancing": { "enabled": false },
        });
        let access_config = json!({
            "authenticationMode": props
                .get("AccessConfig")
                .and_then(|v| v.get("AuthenticationMode"))
                .and_then(|v| v.as_str())
                .unwrap_or("CONFIG_MAP"),
        });
        let upgrade_policy = json!({
            "supportType": props
                .get("UpgradePolicy")
                .and_then(|v| v.get("SupportType"))
                .and_then(|v| v.as_str())
                .unwrap_or("EXTENDED"),
        });
        // Encryption config: convert CFN `[{Provider:{KeyArn}, Resources:[...]}]`
        // into the API's camelCase `[{provider:{keyArn}, resources:[...]}]`.
        let encryption_config = props
            .get("EncryptionConfig")
            .and_then(|v| v.as_array())
            .map(|arr| {
                let items: Vec<Value> = arr
                    .iter()
                    .map(|e| {
                        json!({
                            "provider": {
                                "keyArn": e
                                    .get("Provider")
                                    .and_then(|p| p.get("KeyArn"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or_default(),
                            },
                            "resources": e.get("Resources").cloned().unwrap_or_else(|| json!([])),
                        })
                    })
                    .collect();
                json!(items)
            });

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.clusters.contains_key(&name) {
            return Err(format!("Cluster already exists with name: {name}"));
        }
        let cluster = Cluster {
            name: name.clone(),
            arn: arn.clone(),
            version,
            role_arn,
            status: "ACTIVE".to_string(),
            created_at: Utc::now(),
            endpoint: endpoint.clone(),
            platform_version: "eks.1".to_string(),
            certificate_authority_data: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg==".to_string(),
            resources_vpc_config,
            kubernetes_network_config,
            logging: json!({
                "clusterLogging": [{
                    "types": ["api", "audit", "authenticator", "controllerManager", "scheduler"],
                    "enabled": false,
                }],
            }),
            tags: parse_eks_tags(props.get("Tags")),
            updates: Default::default(),
            connector_config: None,
            encryption_config,
            access_config,
            upgrade_policy,
            compute_config: props.get("ComputeConfig").cloned(),
            storage_config: props.get("StorageConfig").cloned(),
            zonal_shift_config: props.get("ZonalShiftConfig").cloned(),
            remote_network_config: props.get("RemoteNetworkConfig").cloned(),
            control_plane_scaling_config: props.get("ControlPlaneScalingConfig").cloned(),
            deletion_protection: props.get("DeletionProtection").and_then(|v| v.as_bool()),
        };
        let cluster_security_group_id = cluster.resources_vpc_config["clusterSecurityGroupId"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let key_arn = cluster
            .encryption_config
            .as_ref()
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|e| e.get("provider"))
            .and_then(|p| p.get("keyArn"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let ca_data = cluster.certificate_authority_data.clone();
        state.clusters.insert(name.clone(), cluster);

        let oidc = format!(
            "https://oidc.eks.amazonaws.com/id/{}",
            cluster_id_from_endpoint(&endpoint).to_uppercase()
        );
        let mut result = ProvisionResult::new(name)
            .with("Arn", arn)
            .with("Endpoint", endpoint.clone())
            .with("CertificateAuthorityData", ca_data)
            .with("ClusterSecurityGroupId", cluster_security_group_id)
            .with("OpenIdConnectIssuerUrl", oidc)
            .with("Id", cluster_id_from_endpoint(&endpoint));
        if let Some(k) = key_arn {
            result = result.with("EncryptionConfigKeyArn", k);
        }
        Ok(result)
    }

    pub(super) fn delete_eks_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.clusters.remove(physical_id);
        Ok(())
    }

    pub(super) fn get_att_eks_cluster(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let accounts = self.eks_state.read();
        let state = accounts.get(&self.account_id)?;
        let cluster = state.clusters.get(physical_id)?;
        let id = cluster_id_from_endpoint(&cluster.endpoint);
        match attribute {
            "Arn" => Some(cluster.arn.clone()),
            "Endpoint" => Some(cluster.endpoint.clone()),
            "CertificateAuthorityData" => Some(cluster.certificate_authority_data.clone()),
            "ClusterSecurityGroupId" => cluster.resources_vpc_config["clusterSecurityGroupId"]
                .as_str()
                .map(|s| s.to_string()),
            "OpenIdConnectIssuerUrl" => Some(format!(
                "https://oidc.eks.amazonaws.com/id/{}",
                id.to_uppercase()
            )),
            "Id" => Some(id),
            "EncryptionConfigKeyArn" => cluster
                .encryption_config
                .as_ref()
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|e| e.get("provider"))
                .and_then(|p| p.get("keyArn"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            _ => None,
        }
    }

    // --- EKS Nodegroup ---

    pub(super) fn create_eks_nodegroup(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .ok_or("ClusterName is required")?
            .to_string();
        let name = props
            .get("NodegroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let node_role = props
            .get("NodeRole")
            .and_then(|v| v.as_str())
            .ok_or("NodeRole is required")?
            .to_string();
        let subnets = props.get("Subnets").cloned().ok_or("Subnets is required")?;

        let id = Uuid::new_v4().to_string();
        let arn = nodegroup_arn(&self.region, &self.account_id, &cluster_name, &name, &id);
        let now = Utc::now();

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cluster_version = state
            .clusters
            .get(&cluster_name)
            .ok_or_else(|| format!("No cluster found for name: {cluster_name}"))?
            .version
            .clone();
        let version = props
            .get("Version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(cluster_version);
        let release_version = props
            .get("ReleaseVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{version}-20240000"));
        let scaling_config = props
            .get("ScalingConfig")
            .map(cfn_scaling_config)
            .unwrap_or_else(|| json!({ "minSize": 1, "maxSize": 2, "desiredSize": 2 }));
        let ng = Nodegroup {
            name: name.clone(),
            arn: arn.clone(),
            cluster_name: cluster_name.clone(),
            version,
            release_version,
            status: "ACTIVE".to_string(),
            capacity_type: props
                .get("CapacityType")
                .and_then(|v| v.as_str())
                .unwrap_or("ON_DEMAND")
                .to_string(),
            ami_type: props
                .get("AmiType")
                .and_then(|v| v.as_str())
                .unwrap_or("AL2023_x86_64_STANDARD")
                .to_string(),
            node_role,
            created_at: now,
            modified_at: now,
            disk_size: props.get("DiskSize").and_then(|v| v.as_i64()).unwrap_or(20),
            scaling_config,
            update_config: props
                .get("UpdateConfig")
                .map(cfn_update_config)
                .unwrap_or_else(|| json!({ "maxUnavailable": 1 })),
            instance_types: props
                .get("InstanceTypes")
                .cloned()
                .unwrap_or_else(|| json!(["t3.medium"])),
            subnets,
            labels: props.get("Labels").cloned().unwrap_or_else(|| json!({})),
            taints: props.get("Taints").cloned().unwrap_or_else(|| json!([])),
            remote_access: props.get("RemoteAccess").cloned(),
            launch_template: props.get("LaunchTemplate").cloned(),
            asg_name: format!("eks-{name}-{}", &id[..8]),
            tags: parse_eks_tags(props.get("Tags")),
            updates: Default::default(),
            node_repair_config: props.get("NodeRepairConfig").cloned(),
            warm_pool_config: None,
        };
        state
            .nodegroups
            .entry(cluster_name.clone())
            .or_default()
            .insert(name.clone(), ng);

        // CFN Ref for a node group is `clusterName/nodegroupName`.
        let physical_id = format!("{cluster_name}/{name}");
        Ok(ProvisionResult::new(physical_id.clone())
            .with("Arn", arn)
            .with("Id", physical_id)
            .with("ClusterName", cluster_name)
            .with("NodegroupName", name))
    }

    pub(super) fn delete_eks_nodegroup(&self, physical_id: &str) -> Result<(), String> {
        let Some((cluster_name, name)) = physical_id.split_once('/') else {
            return Ok(());
        };
        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.nodegroups.get_mut(cluster_name) {
            m.remove(name);
        }
        Ok(())
    }

    pub(super) fn get_att_eks_nodegroup(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (cluster_name, name) = physical_id.split_once('/')?;
        let accounts = self.eks_state.read();
        let ng = accounts
            .get(&self.account_id)?
            .nodegroups
            .get(cluster_name)?
            .get(name)?;
        match attribute {
            "Arn" => Some(ng.arn.clone()),
            "Id" => Some(physical_id.to_string()),
            "ClusterName" => Some(ng.cluster_name.clone()),
            "NodegroupName" => Some(ng.name.clone()),
            _ => None,
        }
    }

    // --- EKS FargateProfile ---

    pub(super) fn create_eks_fargate_profile(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .ok_or("ClusterName is required")?
            .to_string();
        let name = props
            .get("FargateProfileName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let pod_execution_role_arn = props
            .get("PodExecutionRoleArn")
            .and_then(|v| v.as_str())
            .ok_or("PodExecutionRoleArn is required")?
            .to_string();
        let selectors = props
            .get("Selectors")
            .map(cfn_fargate_selectors)
            .unwrap_or_else(|| json!([]));

        let id = Uuid::new_v4().to_string();
        let arn = fargate_profile_arn(&self.region, &self.account_id, &cluster_name, &name, &id);

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.clusters.contains_key(&cluster_name) {
            return Err(format!("No cluster found for name: {cluster_name}"));
        }
        let profile = FargateProfile {
            name: name.clone(),
            arn: arn.clone(),
            cluster_name: cluster_name.clone(),
            pod_execution_role_arn,
            status: "ACTIVE".to_string(),
            created_at: Utc::now(),
            subnets: props.get("Subnets").cloned().unwrap_or_else(|| json!([])),
            selectors,
            tags: parse_eks_tags(props.get("Tags")),
        };
        state
            .fargate_profiles
            .entry(cluster_name.clone())
            .or_default()
            .insert(name.clone(), profile);
        // Physical id encodes cluster + profile so delete can locate it.
        Ok(ProvisionResult::new(format!("{cluster_name}|{name}")).with("Arn", arn))
    }

    pub(super) fn delete_eks_fargate_profile(&self, physical_id: &str) -> Result<(), String> {
        let Some((cluster_name, name)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.fargate_profiles.get_mut(cluster_name) {
            m.remove(name);
        }
        Ok(())
    }

    pub(super) fn get_att_eks_fargate_profile(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (cluster_name, name) = physical_id.split_once('|')?;
        let accounts = self.eks_state.read();
        let profile = accounts
            .get(&self.account_id)?
            .fargate_profiles
            .get(cluster_name)?
            .get(name)?;
        match attribute {
            "Arn" => Some(profile.arn.clone()),
            _ => None,
        }
    }

    // --- EKS Addon ---

    pub(super) fn create_eks_addon(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .ok_or("ClusterName is required")?
            .to_string();
        let name = props
            .get("AddonName")
            .and_then(|v| v.as_str())
            .ok_or("AddonName is required")?
            .to_string();

        let id = Uuid::new_v4().to_string();
        let arn = addon_arn(&self.region, &self.account_id, &cluster_name, &name, &id);
        let now = Utc::now();
        // Pod identity association ARNs, if requested.
        let pod_identity_associations: Vec<String> = props
            .get("PodIdentityAssociations")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|_| {
                        let sid = Uuid::new_v4().to_string().replace('-', "");
                        pod_identity_association_arn(
                            &self.region,
                            &self.account_id,
                            &cluster_name,
                            &sid[..17.min(sid.len())],
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cluster_version = state
            .clusters
            .get(&cluster_name)
            .ok_or_else(|| format!("No cluster found for name: {cluster_name}"))?
            .version
            .clone();
        let addon = Addon {
            name: name.clone(),
            arn: arn.clone(),
            cluster_name: cluster_name.clone(),
            addon_version: props
                .get("AddonVersion")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("v{cluster_version}-eksbuild.1")),
            status: "ACTIVE".to_string(),
            created_at: now,
            modified_at: now,
            service_account_role_arn: props
                .get("ServiceAccountRoleArn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            configuration_values: props
                .get("ConfigurationValues")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            namespace: None,
            pod_identity_associations,
            tags: parse_eks_tags(props.get("Tags")),
            updates: Default::default(),
        };
        state
            .addons
            .entry(cluster_name.clone())
            .or_default()
            .insert(name.clone(), addon);
        Ok(ProvisionResult::new(format!("{cluster_name}|{name}"))
            .with("Arn", arn.clone())
            .with("AddonArn", arn))
    }

    pub(super) fn delete_eks_addon(&self, physical_id: &str) -> Result<(), String> {
        let Some((cluster_name, name)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.addons.get_mut(cluster_name) {
            m.remove(name);
        }
        Ok(())
    }

    pub(super) fn get_att_eks_addon(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let (cluster_name, name) = physical_id.split_once('|')?;
        let accounts = self.eks_state.read();
        let addon = accounts
            .get(&self.account_id)?
            .addons
            .get(cluster_name)?
            .get(name)?;
        match attribute {
            "Arn" | "AddonArn" => Some(addon.arn.clone()),
            _ => None,
        }
    }

    // --- EKS AccessEntry ---

    pub(super) fn create_eks_access_entry(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .ok_or("ClusterName is required")?
            .to_string();
        let principal_arn = props
            .get("PrincipalArn")
            .and_then(|v| v.as_str())
            .ok_or("PrincipalArn is required")?
            .to_string();

        let (principal_type, principal_name) = principal_parts(&principal_arn);
        let id = Uuid::new_v4().to_string();
        let arn = access_entry_arn(
            &self.region,
            &self.account_id,
            &cluster_name,
            &principal_type,
            &principal_name,
            &id,
        );
        let now = Utc::now();
        let kubernetes_groups: Vec<String> = props
            .get("KubernetesGroups")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.clusters.contains_key(&cluster_name) {
            return Err(format!("No cluster found for name: {cluster_name}"));
        }
        let entry = AccessEntry {
            principal_arn: principal_arn.clone(),
            cluster_name: cluster_name.clone(),
            arn: arn.clone(),
            kubernetes_groups,
            username: props
                .get("Username")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| principal_arn.clone()),
            type_: props
                .get("Type")
                .and_then(|v| v.as_str())
                .unwrap_or("STANDARD")
                .to_string(),
            created_at: now,
            modified_at: now,
            tags: parse_eks_tags(props.get("Tags")),
            associated_policies: Vec::new(),
        };
        state
            .access_entries
            .entry(cluster_name.clone())
            .or_default()
            .insert(principal_arn.clone(), entry);
        Ok(
            ProvisionResult::new(format!("{cluster_name}|{principal_arn}"))
                .with("AccessEntryArn", arn),
        )
    }

    pub(super) fn delete_eks_access_entry(&self, physical_id: &str) -> Result<(), String> {
        let Some((cluster_name, principal_arn)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.access_entries.get_mut(cluster_name) {
            m.remove(principal_arn);
        }
        Ok(())
    }

    pub(super) fn get_att_eks_access_entry(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (cluster_name, principal_arn) = physical_id.split_once('|')?;
        let accounts = self.eks_state.read();
        let entry = accounts
            .get(&self.account_id)?
            .access_entries
            .get(cluster_name)?
            .get(principal_arn)?;
        match attribute {
            "AccessEntryArn" => Some(entry.arn.clone()),
            _ => None,
        }
    }

    // --- EKS IdentityProviderConfig ---

    pub(super) fn create_eks_identity_provider_config(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .ok_or("ClusterName is required")?
            .to_string();
        let oidc = props.get("Oidc").ok_or("Oidc is required")?;
        let name = oidc
            .get("IdentityProviderConfigName")
            .and_then(|v| v.as_str())
            .ok_or("Oidc.IdentityProviderConfigName is required")?
            .to_string();
        let issuer_url = oidc
            .get("IssuerUrl")
            .and_then(|v| v.as_str())
            .ok_or("Oidc.IssuerUrl is required")?
            .to_string();
        let client_id = oidc
            .get("ClientId")
            .and_then(|v| v.as_str())
            .ok_or("Oidc.ClientId is required")?
            .to_string();

        let id = Uuid::new_v4().to_string();
        let arn =
            identity_provider_config_arn(&self.region, &self.account_id, &cluster_name, &name, &id);

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.clusters.contains_key(&cluster_name) {
            return Err(format!("No cluster found for name: {cluster_name}"));
        }
        let config = IdentityProviderConfig {
            name: name.clone(),
            arn: arn.clone(),
            cluster_name: cluster_name.clone(),
            issuer_url,
            client_id,
            username_claim: oidc
                .get("UsernameClaim")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            username_prefix: oidc
                .get("UsernamePrefix")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            groups_claim: oidc
                .get("GroupsClaim")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            groups_prefix: oidc
                .get("GroupsPrefix")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            required_claims: oidc
                .get("RequiredClaims")
                .cloned()
                .unwrap_or_else(|| json!({})),
            status: "ACTIVE".to_string(),
            tags: parse_eks_tags(props.get("Tags")),
        };
        state
            .identity_provider_configs
            .entry(cluster_name.clone())
            .or_default()
            .insert(name.clone(), config);
        Ok(ProvisionResult::new(format!("{cluster_name}|{name}"))
            .with("IdentityProviderConfigArn", arn))
    }

    pub(super) fn delete_eks_identity_provider_config(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let Some((cluster_name, name)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.identity_provider_configs.get_mut(cluster_name) {
            m.remove(name);
        }
        Ok(())
    }

    pub(super) fn get_att_eks_identity_provider_config(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (cluster_name, name) = physical_id.split_once('|')?;
        let accounts = self.eks_state.read();
        let config = accounts
            .get(&self.account_id)?
            .identity_provider_configs
            .get(cluster_name)?
            .get(name)?;
        match attribute {
            "IdentityProviderConfigArn" => Some(config.arn.clone()),
            _ => None,
        }
    }

    // --- EKS PodIdentityAssociation ---

    pub(super) fn create_eks_pod_identity_association(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .ok_or("ClusterName is required")?
            .to_string();
        let namespace = props
            .get("Namespace")
            .and_then(|v| v.as_str())
            .ok_or("Namespace is required")?
            .to_string();
        let service_account = props
            .get("ServiceAccount")
            .and_then(|v| v.as_str())
            .ok_or("ServiceAccount is required")?
            .to_string();
        let role_arn = props
            .get("RoleArn")
            .and_then(|v| v.as_str())
            .ok_or("RoleArn is required")?
            .to_string();

        let suffix = Uuid::new_v4().to_string().replace('-', "");
        let suffix = &suffix[..17.min(suffix.len())];
        let association_id = format!("a-{suffix}");
        let association_arn =
            pod_identity_association_arn(&self.region, &self.account_id, &cluster_name, suffix);
        let now = Utc::now();

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.clusters.contains_key(&cluster_name) {
            return Err(format!("No cluster found for name: {cluster_name}"));
        }
        let assoc = PodIdentityAssociation {
            cluster_name: cluster_name.clone(),
            namespace,
            service_account,
            role_arn,
            association_arn: association_arn.clone(),
            association_id: association_id.clone(),
            created_at: now,
            modified_at: now,
            disable_session_tags: props
                .get("DisableSessionTags")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            target_role_arn: props
                .get("TargetRoleArn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            external_id: None,
            tags: parse_eks_tags(props.get("Tags")),
        };
        state
            .pod_identity_associations
            .entry(cluster_name.clone())
            .or_default()
            .insert(association_id.clone(), assoc);
        Ok(
            ProvisionResult::new(format!("{cluster_name}|{association_id}"))
                .with("AssociationArn", association_arn)
                .with("AssociationId", association_id),
        )
    }

    pub(super) fn delete_eks_pod_identity_association(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let Some((cluster_name, association_id)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.pod_identity_associations.get_mut(cluster_name) {
            m.remove(association_id);
        }
        Ok(())
    }

    pub(super) fn get_att_eks_pod_identity_association(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let (cluster_name, association_id) = physical_id.split_once('|')?;
        let accounts = self.eks_state.read();
        let assoc = accounts
            .get(&self.account_id)?
            .pod_identity_associations
            .get(cluster_name)?
            .get(association_id)?;
        match attribute {
            "AssociationArn" => Some(assoc.association_arn.clone()),
            "AssociationId" => Some(assoc.association_id.clone()),
            _ => None,
        }
    }

    // --- In-place UpdateStack arms ---
    //
    // Each mutates the owning EKS record in place (applying the mutable
    // properties, preserving the physical id / ARN / created_at) instead of the
    // reprovision fallback's delete+recreate. A recreate would mint new ARNs and,
    // for a Cluster, orphan every nodegroup/addon/profile keyed under its name.

    pub(super) fn update_eks_cluster(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = existing.physical_id.clone();

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cluster = state
            .clusters
            .get_mut(&name)
            .ok_or_else(|| format!("No cluster found for name: {name}"))?;

        if let Some(v) = props.get("Version").and_then(|v| v.as_str()) {
            cluster.version = v.to_string();
        }
        if props.get("Logging").is_some() {
            // Echo the CFN logging block as-is; the direct service also stores
            // whatever `UpdateClusterConfig` sends.
            cluster.logging = props.get("Logging").cloned().unwrap_or(json!({}));
        }
        if let Some(mode) = props
            .get("AccessConfig")
            .and_then(|v| v.get("AuthenticationMode"))
            .and_then(|v| v.as_str())
        {
            cluster.access_config = json!({ "authenticationMode": mode });
        }
        if let Some(st) = props
            .get("UpgradePolicy")
            .and_then(|v| v.get("SupportType"))
            .and_then(|v| v.as_str())
        {
            cluster.upgrade_policy = json!({ "supportType": st });
        }
        if let Some(v) = props.get("DeletionProtection").and_then(|v| v.as_bool()) {
            cluster.deletion_protection = Some(v);
        }
        // Endpoint public/private access are mutable via UpdateClusterConfig.
        if let Some(vpc) = props.get("ResourcesVpcConfig") {
            if let Some(obj) = cluster.resources_vpc_config.as_object_mut() {
                if let Some(v) = vpc.get("EndpointPublicAccess").and_then(|v| v.as_bool()) {
                    obj.insert("endpointPublicAccess".to_string(), json!(v));
                }
                if let Some(v) = vpc.get("EndpointPrivateAccess").and_then(|v| v.as_bool()) {
                    obj.insert("endpointPrivateAccess".to_string(), json!(v));
                }
                if let Some(v) = vpc.get("PublicAccessCidrs") {
                    obj.insert("publicAccessCidrs".to_string(), v.clone());
                }
            }
        }
        if props.get("Tags").is_some() {
            cluster.tags = parse_eks_tags(props.get("Tags"));
        }

        let arn = cluster.arn.clone();
        let endpoint = cluster.endpoint.clone();
        let ca_data = cluster.certificate_authority_data.clone();
        let cluster_security_group_id = cluster.resources_vpc_config["clusterSecurityGroupId"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let key_arn = cluster
            .encryption_config
            .as_ref()
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|e| e.get("provider"))
            .and_then(|p| p.get("keyArn"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let id = cluster_id_from_endpoint(&endpoint);
        let oidc = format!("https://oidc.eks.amazonaws.com/id/{}", id.to_uppercase());
        let mut result = ProvisionResult::new(name)
            .with("Arn", arn)
            .with("Endpoint", endpoint)
            .with("CertificateAuthorityData", ca_data)
            .with("ClusterSecurityGroupId", cluster_security_group_id)
            .with("OpenIdConnectIssuerUrl", oidc)
            .with("Id", id);
        if let Some(k) = key_arn {
            result = result.with("EncryptionConfigKeyArn", k);
        }
        Ok(result)
    }

    pub(super) fn update_eks_nodegroup(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let (cluster_name, name) = physical_id
            .split_once('/')
            .map(|(c, n)| (c.to_string(), n.to_string()))
            .ok_or_else(|| format!("Invalid Nodegroup physical id: {physical_id}"))?;

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let ng = state
            .nodegroups
            .get_mut(&cluster_name)
            .and_then(|m| m.get_mut(&name))
            .ok_or_else(|| format!("No nodegroup found: {physical_id}"))?;

        if let Some(v) = props.get("ScalingConfig") {
            ng.scaling_config = cfn_scaling_config(v);
        }
        if let Some(v) = props.get("UpdateConfig") {
            ng.update_config = cfn_update_config(v);
        }
        if let Some(v) = props.get("Labels") {
            ng.labels = v.clone();
        }
        if let Some(v) = props.get("Taints") {
            ng.taints = v.clone();
        }
        if let Some(v) = props.get("Version").and_then(|v| v.as_str()) {
            ng.version = v.to_string();
        }
        if let Some(v) = props.get("ReleaseVersion").and_then(|v| v.as_str()) {
            ng.release_version = v.to_string();
        }
        if props.get("Tags").is_some() {
            ng.tags = parse_eks_tags(props.get("Tags"));
        }
        ng.modified_at = Utc::now();

        let arn = ng.arn.clone();
        Ok(ProvisionResult::new(physical_id.clone())
            .with("Arn", arn)
            .with("Id", physical_id)
            .with("ClusterName", cluster_name)
            .with("NodegroupName", name))
    }

    pub(super) fn update_eks_fargate_profile(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let (cluster_name, name) = physical_id
            .split_once('|')
            .ok_or_else(|| format!("Invalid FargateProfile physical id: {physical_id}"))?;

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let profile = state
            .fargate_profiles
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(|| format!("No fargate profile found: {physical_id}"))?;
        // Fargate profiles are immutable except for their tags.
        if props.get("Tags").is_some() {
            profile.tags = parse_eks_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(physical_id).with("Arn", profile.arn.clone()))
    }

    pub(super) fn update_eks_addon(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let (cluster_name, name) = physical_id
            .split_once('|')
            .ok_or_else(|| format!("Invalid Addon physical id: {physical_id}"))?;

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let addon = state
            .addons
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(|| format!("No addon found: {physical_id}"))?;
        if let Some(v) = props.get("AddonVersion").and_then(|v| v.as_str()) {
            addon.addon_version = v.to_string();
        }
        if let Some(v) = props.get("ConfigurationValues").and_then(|v| v.as_str()) {
            addon.configuration_values = Some(v.to_string());
        }
        if let Some(v) = props.get("ServiceAccountRoleArn").and_then(|v| v.as_str()) {
            addon.service_account_role_arn = Some(v.to_string());
        }
        if props.get("Tags").is_some() {
            addon.tags = parse_eks_tags(props.get("Tags"));
        }
        addon.modified_at = Utc::now();
        let arn = addon.arn.clone();
        Ok(ProvisionResult::new(physical_id)
            .with("Arn", arn.clone())
            .with("AddonArn", arn))
    }

    pub(super) fn update_eks_access_entry(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let (cluster_name, principal_arn) = physical_id
            .split_once('|')
            .ok_or_else(|| format!("Invalid AccessEntry physical id: {physical_id}"))?;

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let entry = state
            .access_entries
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(principal_arn))
            .ok_or_else(|| format!("No access entry found: {physical_id}"))?;
        if let Some(v) = props.get("KubernetesGroups").and_then(|v| v.as_array()) {
            entry.kubernetes_groups = v
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(v) = props.get("Username").and_then(|v| v.as_str()) {
            entry.username = v.to_string();
        }
        if props.get("Tags").is_some() {
            entry.tags = parse_eks_tags(props.get("Tags"));
        }
        entry.modified_at = Utc::now();
        Ok(ProvisionResult::new(physical_id).with("AccessEntryArn", entry.arn.clone()))
    }

    pub(super) fn update_eks_identity_provider_config(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let (cluster_name, name) = physical_id
            .split_once('|')
            .ok_or_else(|| format!("Invalid IdentityProviderConfig physical id: {physical_id}"))?;

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let config = state
            .identity_provider_configs
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(name))
            .ok_or_else(|| format!("No identity provider config found: {physical_id}"))?;
        // OIDC config is immutable except for its tags; re-reading (no delete) is
        // still correct -- it stops the destructive reprovision.
        if props.get("Tags").is_some() {
            config.tags = parse_eks_tags(props.get("Tags"));
        }
        Ok(ProvisionResult::new(physical_id).with("IdentityProviderConfigArn", config.arn.clone()))
    }

    pub(super) fn update_eks_pod_identity_association(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let physical_id = existing.physical_id.clone();
        let (cluster_name, association_id) = physical_id
            .split_once('|')
            .ok_or_else(|| format!("Invalid PodIdentityAssociation physical id: {physical_id}"))?;

        let mut accounts = self.eks_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let assoc = state
            .pod_identity_associations
            .get_mut(cluster_name)
            .and_then(|m| m.get_mut(association_id))
            .ok_or_else(|| format!("No pod identity association found: {physical_id}"))?;
        if let Some(v) = props.get("RoleArn").and_then(|v| v.as_str()) {
            assoc.role_arn = v.to_string();
        }
        if let Some(v) = props.get("DisableSessionTags").and_then(|v| v.as_bool()) {
            assoc.disable_session_tags = v;
        }
        if let Some(v) = props.get("TargetRoleArn").and_then(|v| v.as_str()) {
            assoc.target_role_arn = Some(v.to_string());
        }
        if props.get("Tags").is_some() {
            assoc.tags = parse_eks_tags(props.get("Tags"));
        }
        assoc.modified_at = Utc::now();
        Ok(ProvisionResult::new(physical_id)
            .with("AssociationArn", assoc.association_arn.clone())
            .with("AssociationId", assoc.association_id.clone()))
    }
}

/// Split a principal ARN into its `(type, name)` parts, matching the direct EKS
/// service's access-entry ARN derivation (`role`/`user` + trailing name).
fn principal_parts(principal_arn: &str) -> (String, String) {
    let name = principal_arn.rsplit('/').next().unwrap_or(principal_arn);
    let type_ = if principal_arn.contains(":user/") {
        "user"
    } else {
        "role"
    };
    (type_.to_string(), name.to_string())
}

/// Convert a CFN `ScalingConfig` (`{MinSize,MaxSize,DesiredSize}`) into the
/// API's camelCase echo shape.
fn cfn_scaling_config(v: &Value) -> Value {
    json!({
        "minSize": v.get("MinSize").and_then(|x| x.as_i64()).unwrap_or(1),
        "maxSize": v.get("MaxSize").and_then(|x| x.as_i64()).unwrap_or(2),
        "desiredSize": v.get("DesiredSize").and_then(|x| x.as_i64()).unwrap_or(2),
    })
}

/// Convert a CFN nodegroup `UpdateConfig` into the API's camelCase echo shape.
fn cfn_update_config(v: &Value) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(n) = v.get("MaxUnavailable").and_then(|x| x.as_i64()) {
        out.insert("maxUnavailable".to_string(), json!(n));
    }
    if let Some(n) = v.get("MaxUnavailablePercentage").and_then(|x| x.as_i64()) {
        out.insert("maxUnavailablePercentage".to_string(), json!(n));
    }
    if out.is_empty() {
        out.insert("maxUnavailable".to_string(), json!(1));
    }
    Value::Object(out)
}

/// Convert a CFN Fargate `Selectors` list (`[{Namespace,Labels}]`) into the
/// API's camelCase echo shape (`[{namespace,labels}]`).
fn cfn_fargate_selectors(v: &Value) -> Value {
    let items: Vec<Value> = v
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|s| {
                    let mut out = serde_json::Map::new();
                    if let Some(ns) = s.get("Namespace") {
                        out.insert("namespace".to_string(), ns.clone());
                    }
                    if let Some(labels) = s.get("Labels") {
                        out.insert("labels".to_string(), labels.clone());
                    }
                    Value::Object(out)
                })
                .collect()
        })
        .unwrap_or_default();
    json!(items)
}
