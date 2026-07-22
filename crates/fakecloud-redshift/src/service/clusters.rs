//! Cluster lifecycle: create / describe / modify / delete plus the cluster
//! action ops (reboot, pause, resume, resize, rotate-key, IAM roles, …).

use chrono::Utc;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::helpers::*;
use super::RedshiftService;
use crate::state::Cluster;

/// Build the synthetic-but-well-formed public endpoint address for a cluster.
fn endpoint_address(cluster_id: &str, region: &str) -> String {
    let token = &fakecloud_core::ids::short_id(12);
    format!("{cluster_id}.{token}.{region}.redshift.amazonaws.com")
}

pub(super) fn render_cluster(c: &Cluster) -> String {
    let security_groups: String = c
        .cluster_security_groups
        .iter()
        .map(|g| {
            format!(
                "<ClusterSecurityGroup><ClusterSecurityGroupName>{}</ClusterSecurityGroupName><Status>active</Status></ClusterSecurityGroup>",
                xml_escape(g)
            )
        })
        .collect();
    let vpc_sgs: String = c
        .vpc_security_group_ids
        .iter()
        .map(|g| {
            format!(
                "<VpcSecurityGroup><VpcSecurityGroupId>{}</VpcSecurityGroupId><Status>active</Status></VpcSecurityGroup>",
                xml_escape(g)
            )
        })
        .collect();
    let iam_roles: String = c
        .iam_roles
        .iter()
        .map(|r| {
            format!(
                "<ClusterIamRole><IamRoleArn>{}</IamRoleArn><ApplyStatus>in-sync</ApplyStatus></ClusterIamRole>",
                xml_escape(r)
            )
        })
        .collect();
    format!(
        "<ClusterIdentifier>{id}</ClusterIdentifier>\
         <NodeType>{node_type}</NodeType>\
         <ClusterStatus>{status}</ClusterStatus>\
         <ClusterAvailabilityStatus>{avail}</ClusterAvailabilityStatus>\
         <MasterUsername>{master}</MasterUsername>\
         <DBName>{db}</DBName>\
         <Endpoint><Address>{addr}</Address><Port>{port}</Port></Endpoint>\
         <ClusterCreateTime>{created}</ClusterCreateTime>\
         <AutomatedSnapshotRetentionPeriod>{asrp}</AutomatedSnapshotRetentionPeriod>\
         <ManualSnapshotRetentionPeriod>{msrp}</ManualSnapshotRetentionPeriod>\
         <ClusterSecurityGroups>{security_groups}</ClusterSecurityGroups>\
         <VpcSecurityGroups>{vpc_sgs}</VpcSecurityGroups>\
         <ClusterParameterGroups><ClusterParameterGroup><ParameterGroupName>{pg}</ParameterGroupName><ParameterApplyStatus>in-sync</ParameterApplyStatus></ClusterParameterGroup></ClusterParameterGroups>\
         {subnet_group}\
         {vpc_id}\
         <AvailabilityZone>{az}</AvailabilityZone>\
         <PreferredMaintenanceWindow>{pmw}</PreferredMaintenanceWindow>\
         <ClusterVersion>{version}</ClusterVersion>\
         <ClusterRevisionNumber>1</ClusterRevisionNumber>\
         <ClusterPublicKey>ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQfakecloud Amazon-Redshift\n</ClusterPublicKey>\
         <AllowVersionUpgrade>{avu}</AllowVersionUpgrade>\
         <NumberOfNodes>{nodes}</NumberOfNodes>\
         <PubliclyAccessible>{public}</PubliclyAccessible>\
         <Encrypted>{encrypted}</Encrypted>\
         <MultiAZ>{multi_az}</MultiAZ>\
         <ClusterNodes><member><NodeRole>LEADER</NodeRole><PrivateIPAddress>10.0.0.1</PrivateIPAddress><PublicIPAddress>54.0.0.1</PublicIPAddress></member></ClusterNodes>\
         <EnhancedVpcRouting>{evr}</EnhancedVpcRouting>\
         <MaintenanceTrackName>{track}</MaintenanceTrackName>\
         <AvailabilityZoneRelocationStatus>{azr}</AvailabilityZoneRelocationStatus>\
         <AquaConfiguration><AquaStatus>{aqua}</AquaStatus><AquaConfigurationStatus>{aqua_cfg}</AquaConfigurationStatus></AquaConfiguration>\
         <TotalStorageCapacityInMegaBytes>{storage}</TotalStorageCapacityInMegaBytes>\
         {default_iam}\
         <IamRoles>{iam_roles}</IamRoles>\
         {schedule}\
         {snapshot_copy}\
         {kms}\
         {tags}",
        id = xml_escape(&c.cluster_identifier),
        node_type = xml_escape(&c.node_type),
        status = xml_escape(&c.cluster_status),
        avail = xml_escape(&c.cluster_availability_status),
        master = xml_escape(&c.master_username),
        db = xml_escape(&c.db_name),
        addr = xml_escape(&c.endpoint_address),
        port = c.endpoint_port,
        created = c.cluster_create_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        asrp = c.automated_snapshot_retention_period,
        msrp = c.manual_snapshot_retention_period,
        pg = xml_escape(&c.cluster_parameter_group_name),
        subnet_group = opt_elem("ClusterSubnetGroupName", c.cluster_subnet_group_name.as_deref()),
        vpc_id = opt_elem("VpcId", c.vpc_id.as_deref()),
        az = xml_escape(&c.availability_zone),
        pmw = xml_escape(&c.preferred_maintenance_window),
        version = xml_escape(&c.cluster_version),
        avu = c.allow_version_upgrade,
        nodes = c.number_of_nodes,
        public = c.publicly_accessible,
        encrypted = c.encrypted,
        multi_az = if c.multi_az { "Enabled" } else { "Disabled" },
        evr = c.enhanced_vpc_routing,
        track = xml_escape(&c.maintenance_track_name),
        azr = xml_escape(&c.availability_zone_relocation_status),
        aqua = "disabled",
        aqua_cfg = xml_escape(&c.aqua_configuration_status),
        storage = c.total_storage_capacity_in_mega_bytes,
        default_iam = opt_elem("DefaultIamRoleArn", c.default_iam_role_arn.as_deref()),
        schedule = opt_elem("SnapshotScheduleIdentifier", c.snapshot_schedule_identifier.as_deref()),
        snapshot_copy = c
            .snapshot_copy
            .as_ref()
            .map(|s| {
                format!(
                    "<ClusterSnapshotCopyStatus><DestinationRegion>{region}</DestinationRegion>\
                     <RetentionPeriod>{retention}</RetentionPeriod>\
                     <ManualSnapshotRetentionPeriod>{manual}</ManualSnapshotRetentionPeriod>{grant}</ClusterSnapshotCopyStatus>",
                    region = xml_escape(&s.destination_region),
                    retention = s.retention_period,
                    manual = s.manual_snapshot_retention_period,
                    grant = opt_elem("SnapshotCopyGrantName", s.snapshot_copy_grant_name.as_deref()),
                )
            })
            .unwrap_or_default(),
        kms = opt_elem("KmsKeyId", c.kms_key_id.as_deref()),
        tags = render_tags(&c.tags),
    )
}

impl RedshiftService {
    pub(super) fn create_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let node_type = param(req, "NodeType").unwrap_or_default();
        let master = param(req, "MasterUsername").unwrap_or_default();
        let cluster_type = param_or(req, "ClusterType", "multi-node");
        let number_of_nodes = int_param(req, "NumberOfNodes")
            .unwrap_or(if cluster_type == "single-node" { 1 } else { 2 });
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.clusters.contains_key(&id) {
            return Err(cluster_already_exists(&id));
        }
        let cluster = Cluster {
            cluster_identifier: id.clone(),
            node_type,
            cluster_status: "available".to_string(),
            cluster_availability_status: "Available".to_string(),
            master_username: master,
            db_name: param_or(req, "DBName", "dev"),
            endpoint_address: endpoint_address(&id, &req.region),
            endpoint_port: int_param(req, "Port").unwrap_or(5439),
            cluster_create_time: Utc::now(),
            automated_snapshot_retention_period: int_param(req, "AutomatedSnapshotRetentionPeriod")
                .unwrap_or(1),
            manual_snapshot_retention_period: int_param(req, "ManualSnapshotRetentionPeriod")
                .unwrap_or(-1),
            cluster_security_groups: member_list(
                req,
                "ClusterSecurityGroups",
                "ClusterSecurityGroupName",
            ),
            vpc_security_group_ids: member_list(req, "VpcSecurityGroupIds", "VpcSecurityGroupId"),
            cluster_parameter_group_name: param_or(
                req,
                "ClusterParameterGroupName",
                "default.redshift-1.0",
            ),
            cluster_subnet_group_name: param(req, "ClusterSubnetGroupName"),
            vpc_id: param(req, "ClusterSubnetGroupName").map(|_| "vpc-fakecloud".to_string()),
            availability_zone: param_or(req, "AvailabilityZone", &format!("{}a", req.region)),
            preferred_maintenance_window: param_or(
                req,
                "PreferredMaintenanceWindow",
                "sat:06:00-sat:06:30",
            ),
            cluster_version: param_or(req, "ClusterVersion", "1.0"),
            allow_version_upgrade: bool_param(req, "AllowVersionUpgrade").unwrap_or(true),
            number_of_nodes,
            publicly_accessible: bool_param(req, "PubliclyAccessible").unwrap_or(false),
            encrypted: bool_param(req, "Encrypted").unwrap_or(false),
            multi_az: bool_param(req, "MultiAZ").unwrap_or(false),
            cluster_type,
            kms_key_id: param(req, "KmsKeyId"),
            enhanced_vpc_routing: bool_param(req, "EnhancedVpcRouting").unwrap_or(false),
            maintenance_track_name: param_or(req, "MaintenanceTrackName", "current"),
            elastic_ip: param(req, "ElasticIp"),
            availability_zone_relocation_status: if bool_param(req, "AvailabilityZoneRelocation")
                .unwrap_or(false)
            {
                "enabled".to_string()
            } else {
                "disabled".to_string()
            },
            aqua_configuration_status: param_or(req, "AquaConfigurationStatus", "auto"),
            default_iam_role_arn: param(req, "DefaultIamRoleArn"),
            iam_roles: member_list(req, "IamRoles", "IamRoleArn"),
            snapshot_schedule_identifier: param(req, "SnapshotScheduleIdentifier"),
            total_storage_capacity_in_mega_bytes: 0,
            next_maintenance_window_start_time: None,
            snapshot_copy: None,
            tags: parse_tags(req),
        };
        acct.clusters.insert(id, cluster.clone());
        Ok(xml_resp(
            "CreateCluster",
            format!("<Cluster>{}</Cluster>", render_cluster(&cluster)),
            &req.request_id,
        ))
    }

    pub(super) fn describe_clusters(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let all: Vec<_> = if let Some(id) = param(req, "ClusterIdentifier") {
            match acct.clusters.get(&id) {
                Some(c) => vec![c.clone()],
                None => return Err(cluster_not_found(&id)),
            }
        } else {
            acct.clusters.values().cloned().collect()
        };
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|c| format!("<Cluster>{}</Cluster>", render_cluster(c)))
            .collect();
        Ok(xml_resp(
            "DescribeClusters",
            format!("{}<Clusters>{inner}</Clusters>", render_marker(next)),
            &req.request_id,
        ))
    }

    pub(super) fn modify_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let c = acct
            .clusters
            .get_mut(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        if let Some(v) = param(req, "NodeType") {
            c.node_type = v;
        }
        if let Some(v) = int_param(req, "NumberOfNodes") {
            c.number_of_nodes = v;
        }
        if let Some(v) = param(req, "ClusterType") {
            c.cluster_type = v;
        }
        if let Some(v) = param(req, "MasterUserPassword") {
            let _ = v; // password is never echoed back
        }
        if let Some(v) = int_param(req, "AutomatedSnapshotRetentionPeriod") {
            c.automated_snapshot_retention_period = v;
        }
        if let Some(v) = int_param(req, "ManualSnapshotRetentionPeriod") {
            c.manual_snapshot_retention_period = v;
        }
        if let Some(v) = param(req, "PreferredMaintenanceWindow") {
            c.preferred_maintenance_window = v;
        }
        if let Some(v) = param(req, "ClusterVersion") {
            c.cluster_version = v;
        }
        if let Some(v) = bool_param(req, "AllowVersionUpgrade") {
            c.allow_version_upgrade = v;
        }
        if let Some(v) = bool_param(req, "PubliclyAccessible") {
            c.publicly_accessible = v;
        }
        if let Some(v) = param(req, "ClusterParameterGroupName") {
            c.cluster_parameter_group_name = v;
        }
        if let Some(v) = bool_param(req, "EnhancedVpcRouting") {
            c.enhanced_vpc_routing = v;
        }
        if let Some(v) = bool_param(req, "MultiAZ") {
            c.multi_az = v;
        }
        if let Some(v) = param(req, "MaintenanceTrackName") {
            c.maintenance_track_name = v;
        }
        if let Some(v) = bool_param(req, "Encrypted") {
            c.encrypted = v;
        }
        if let Some(v) = param(req, "KmsKeyId") {
            c.kms_key_id = Some(v);
        }
        if let Some(v) = param(req, "ElasticIp") {
            c.elastic_ip = Some(v);
        }
        if let Some(v) = int_param(req, "Port") {
            c.endpoint_port = v;
        }
        if let Some(v) = param(req, "AvailabilityZone") {
            c.availability_zone = v;
        }
        if let Some(v) = bool_param(req, "AvailabilityZoneRelocation") {
            c.availability_zone_relocation_status = if v {
                "enabled".to_string()
            } else {
                "disabled".to_string()
            };
        }
        let sgs = member_list(req, "ClusterSecurityGroups", "ClusterSecurityGroupName");
        if !sgs.is_empty() {
            c.cluster_security_groups = sgs;
        }
        let vpc_sgs = member_list(req, "VpcSecurityGroupIds", "VpcSecurityGroupId");
        if !vpc_sgs.is_empty() {
            c.vpc_security_group_ids = vpc_sgs;
        }
        // A rename stamps the new identifier onto the cluster before it is
        // rendered so the response echoes the new name.
        let new_id = param(req, "NewClusterIdentifier").filter(|n| n != &id);
        if let Some(nid) = &new_id {
            c.cluster_identifier = nid.clone();
        }
        let out = render_cluster(c);
        // Re-key the cluster in the map so subsequent Describe/Modify/Delete
        // resolve under the new identifier (the old name 404s).
        if let Some(nid) = new_id {
            let renamed = acct.clusters.remove(&id).expect("cluster fetched above");
            acct.clusters.insert(nid, renamed);
        }
        Ok(xml_resp(
            "ModifyCluster",
            format!("<Cluster>{out}</Cluster>"),
            &req.request_id,
        ))
    }

    pub(super) fn delete_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let mut c = acct
            .clusters
            .remove(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        c.cluster_status = "deleting".to_string();
        Ok(xml_resp(
            "DeleteCluster",
            format!("<Cluster>{}</Cluster>", render_cluster(&c)),
            &req.request_id,
        ))
    }

    /// Shared implementation for the state-transition action ops (reboot,
    /// pause, resume, rotate-key, …): look the cluster up, optionally set a
    /// transient status, and echo it back under `<Cluster>`.
    fn cluster_action(
        &self,
        req: &AwsRequest,
        action: &str,
        transient_status: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let c = acct
            .clusters
            .get_mut(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        if let Some(s) = transient_status {
            c.cluster_status = s.to_string();
        }
        let out = render_cluster(c);
        Ok(xml_resp(
            action,
            format!("<Cluster>{out}</Cluster>"),
            &req.request_id,
        ))
    }

    pub(super) fn reboot_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "RebootCluster", Some("rebooting"))
    }

    pub(super) fn pause_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "PauseCluster", Some("pausing"))
    }

    pub(super) fn resume_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "ResumeCluster", Some("resuming"))
    }

    pub(super) fn resize_cluster(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        {
            let id = param(req, "ClusterIdentifier").unwrap_or_default();
            let mut guard = self.state.write();
            let acct = guard.account(&req.account_id);
            let c = acct
                .clusters
                .get_mut(&id)
                .ok_or_else(|| cluster_not_found(&id))?;
            if let Some(v) = param(req, "NodeType") {
                c.node_type = v;
            }
            if let Some(v) = int_param(req, "NumberOfNodes") {
                c.number_of_nodes = v;
            }
            if let Some(v) = param(req, "ClusterType") {
                c.cluster_type = v;
            }
            c.cluster_status = "resizing".to_string();
        }
        self.cluster_action(req, "ResizeCluster", None)
    }

    pub(super) fn rotate_encryption_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "RotateEncryptionKey", Some("rotating-keys"))
    }

    pub(super) fn modify_cluster_maintenance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "ModifyClusterMaintenance", None)
    }

    pub(super) fn modify_cluster_db_revision(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "ModifyClusterDbRevision", None)
    }

    pub(super) fn failover_primary_compute(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "FailoverPrimaryCompute", Some("failing-over"))
    }

    pub(super) fn modify_cluster_snapshot_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let schedule_id = param(req, "ScheduleIdentifier");
        let disassociate = bool_param(req, "DisassociateSchedule").unwrap_or(false);
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let c = acct
            .clusters
            .get_mut(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        let previous = c.snapshot_schedule_identifier.clone();
        if disassociate {
            c.snapshot_schedule_identifier = None;
        } else if let Some(s) = schedule_id.clone() {
            c.snapshot_schedule_identifier = Some(s);
        }
        // Mirror the association onto the schedule so DescribeSnapshotSchedules
        // reports the cluster under AssociatedClusters (the Terraform provider's
        // aws_redshift_snapshot_schedule_association reads it back from there).
        if let Some(prev) = previous {
            if let Some(sched) = acct.snapshot_schedules.get_mut(&prev) {
                sched.associated_clusters.retain(|cid| cid != &id);
                sched.associated_cluster_count = sched.associated_clusters.len() as i32;
            }
        }
        if !disassociate {
            if let Some(s) = schedule_id {
                if let Some(sched) = acct.snapshot_schedules.get_mut(&s) {
                    if !sched.associated_clusters.contains(&id) {
                        sched.associated_clusters.push(id.clone());
                    }
                    sched.associated_cluster_count = sched.associated_clusters.len() as i32;
                }
            }
        }
        Ok(xml_metadata_only(
            "ModifyClusterSnapshotSchedule",
            &req.request_id,
        ))
    }

    pub(super) fn modify_cluster_iam_roles(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let c = acct
            .clusters
            .get_mut(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        let add = member_list(req, "AddIamRoles", "IamRoleArn");
        let remove = member_list(req, "RemoveIamRoles", "IamRoleArn");
        for r in add {
            if !c.iam_roles.contains(&r) {
                c.iam_roles.push(r);
            }
        }
        c.iam_roles.retain(|r| !remove.contains(r));
        if let Some(arn) = param(req, "DefaultIamRoleArn") {
            c.default_iam_role_arn = Some(arn);
        }
        let out = render_cluster(c);
        Ok(xml_resp(
            "ModifyClusterIamRoles",
            format!("<Cluster>{out}</Cluster>"),
            &req.request_id,
        ))
    }

    pub(super) fn modify_aqua_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let c = acct
            .clusters
            .get_mut(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        if let Some(v) = param(req, "AquaConfigurationStatus") {
            c.aqua_configuration_status = v;
        }
        let status = xml_escape(&c.aqua_configuration_status);
        Ok(xml_resp(
            "ModifyAquaConfiguration",
            format!("<AquaConfiguration><AquaStatus>disabled</AquaStatus><AquaConfigurationStatus>{status}</AquaConfigurationStatus></AquaConfiguration>"),
            &req.request_id,
        ))
    }

    pub(super) fn modify_lakehouse_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.cluster_action(req, "ModifyLakehouseConfiguration", None)
    }

    pub(super) fn cancel_resize(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let c = acct
            .clusters
            .get_mut(&id)
            .ok_or_else(|| cluster_not_found(&id))?;
        c.cluster_status = "available".to_string();
        Ok(xml_resp(
            "CancelResize",
            "<Status>SUCCEEDED</Status><ResizeType>ClassicResize</ResizeType>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_resize(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let guard = self.state.read();
        if !guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.clusters.contains_key(&id))
            .unwrap_or(false)
        {
            return Err(cluster_not_found(&id));
        }
        Ok(xml_resp(
            "DescribeResize",
            "<Status>NONE</Status><ResizeType>ClassicResize</ResizeType>".to_string(),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_db_revisions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let clusters: Vec<Cluster> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.clusters.values().cloned().collect())
            .unwrap_or_default();
        let inner: String = clusters
            .iter()
            .map(|c| {
                format!(
                    "<ClusterDbRevision><ClusterIdentifier>{}</ClusterIdentifier><CurrentDatabaseRevision>1.0</CurrentDatabaseRevision><RevisionTargets/></ClusterDbRevision>",
                    xml_escape(&c.cluster_identifier)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeClusterDbRevisions",
            format!("<ClusterDbRevisions>{inner}</ClusterDbRevisions>"),
            &req.request_id,
        ))
    }

    pub(super) fn get_cluster_credentials(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let db_user = param(req, "DbUser").unwrap_or_default();
        let expiration = (Utc::now() + chrono::Duration::minutes(15))
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        Ok(xml_resp(
            "GetClusterCredentials",
            format!(
                "<DbUser>{}</DbUser><DbPassword>{}</DbPassword><Expiration>{}</Expiration>",
                xml_escape(&db_user),
                xml_escape(&format!("fc-{}", Uuid::new_v4().simple())),
                expiration
            ),
            &req.request_id,
        ))
    }

    pub(super) fn get_cluster_credentials_with_iam(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let expiration = (Utc::now() + chrono::Duration::minutes(15))
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        Ok(xml_resp(
            "GetClusterCredentialsWithIAM",
            format!(
                "<DbUser>IAM:fakecloud</DbUser><DbPassword>{}</DbPassword><Expiration>{expiration}</Expiration><NextRefreshTime>{now}</NextRefreshTime>",
                xml_escape(&format!("fc-{}", Uuid::new_v4().simple()))
            ),
            &req.request_id,
        ))
    }
}
