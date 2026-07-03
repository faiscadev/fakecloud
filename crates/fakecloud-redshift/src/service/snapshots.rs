//! Cluster-snapshot lifecycle plus restore ops.

use chrono::Utc;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::clusters::render_cluster;
use super::helpers::*;
use super::RedshiftService;
use crate::state::{Cluster, Snapshot, TableRestoreStatus};

pub(super) fn render_snapshot(s: &Snapshot) -> String {
    let restore_access: String = s
        .accounts_with_restore_access
        .iter()
        .map(|a| {
            format!(
                "<AccountWithRestoreAccess><AccountId>{}</AccountId></AccountWithRestoreAccess>",
                xml_escape(a)
            )
        })
        .collect();
    format!(
        "<SnapshotIdentifier>{id}</SnapshotIdentifier>\
         <SnapshotArn>{arn}</SnapshotArn>\
         <ClusterIdentifier>{cluster}</ClusterIdentifier>\
         <SnapshotCreateTime>{created}</SnapshotCreateTime>\
         <Status>{status}</Status>\
         <Port>{port}</Port>\
         <AvailabilityZone>{az}</AvailabilityZone>\
         <ClusterCreateTime>{cluster_created}</ClusterCreateTime>\
         <MasterUsername>{master}</MasterUsername>\
         <ClusterVersion>{version}</ClusterVersion>\
         <SnapshotType>{stype}</SnapshotType>\
         <NodeType>{node_type}</NodeType>\
         <NumberOfNodes>{nodes}</NumberOfNodes>\
         <DBName>{db}</DBName>\
         {vpc}\
         <Encrypted>{encrypted}</Encrypted>\
         <ManualSnapshotRetentionPeriod>{msrp}</ManualSnapshotRetentionPeriod>\
         <TotalBackupSizeInMegaBytes>{total}</TotalBackupSizeInMegaBytes>\
         <ActualIncrementalBackupSizeInMegaBytes>{incr}</ActualIncrementalBackupSizeInMegaBytes>\
         <EstimatedSecondsToCompletion>0</EstimatedSecondsToCompletion>\
         <BackupProgressInMegaBytes>{total}</BackupProgressInMegaBytes>\
         <CurrentBackupRateInMegaBytesPerSecond>0.0</CurrentBackupRateInMegaBytesPerSecond>\
         <OwnerAccount>{owner}</OwnerAccount>\
         <AccountsWithRestoreAccess>{restore_access}</AccountsWithRestoreAccess>\
         {kms}\
         {source_region}\
         {tags}",
        id = xml_escape(&s.snapshot_identifier),
        arn = xml_escape(&s.snapshot_arn),
        cluster = xml_escape(&s.cluster_identifier),
        created = s.snapshot_create_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        status = xml_escape(&s.status),
        port = s.port,
        az = xml_escape(&s.availability_zone),
        cluster_created = s.cluster_create_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        master = xml_escape(&s.master_username),
        version = xml_escape(&s.cluster_version),
        stype = xml_escape(&s.snapshot_type),
        node_type = xml_escape(&s.node_type),
        nodes = s.number_of_nodes,
        db = xml_escape(&s.db_name),
        vpc = opt_elem("VpcId", s.vpc_id.as_deref()),
        encrypted = s.encrypted,
        msrp = s.manual_snapshot_retention_period,
        total = s.total_backup_size_in_mega_bytes,
        incr = s.actual_incremental_backup_size_in_mega_bytes,
        owner = xml_escape(&s.owner_account),
        kms = opt_elem("KmsKeyId", s.kms_key_id.as_deref()),
        source_region = opt_elem("SourceRegion", s.source_region.as_deref()),
        tags = render_tags(&s.tags),
    )
}

fn snapshot_from_cluster(
    cluster: &Cluster,
    id: &str,
    stype: &str,
    owner: &str,
    region: &str,
) -> Snapshot {
    Snapshot {
        snapshot_identifier: id.to_string(),
        snapshot_arn: format!(
            "arn:aws:redshift:{region}:{owner}:snapshot:{}/{id}",
            cluster.cluster_identifier
        ),
        cluster_identifier: cluster.cluster_identifier.clone(),
        snapshot_create_time: Utc::now(),
        status: "available".to_string(),
        port: cluster.endpoint_port,
        availability_zone: cluster.availability_zone.clone(),
        cluster_create_time: cluster.cluster_create_time,
        master_username: cluster.master_username.clone(),
        cluster_version: cluster.cluster_version.clone(),
        snapshot_type: stype.to_string(),
        node_type: cluster.node_type.clone(),
        number_of_nodes: cluster.number_of_nodes,
        db_name: cluster.db_name.clone(),
        vpc_id: cluster.vpc_id.clone(),
        encrypted: cluster.encrypted,
        kms_key_id: cluster.kms_key_id.clone(),
        manual_snapshot_retention_period: -1,
        total_backup_size_in_mega_bytes: 0.0,
        actual_incremental_backup_size_in_mega_bytes: 0.0,
        accounts_with_restore_access: Vec::new(),
        owner_account: owner.to_string(),
        source_region: None,
        tags: Vec::new(),
    }
}

impl RedshiftService {
    pub(super) fn create_cluster_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let snap_id = param(req, "SnapshotIdentifier").unwrap_or_default();
        let cluster_id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.snapshots.contains_key(&snap_id) {
            return Err(snapshot_already_exists(&snap_id));
        }
        let cluster = acct
            .clusters
            .get(&cluster_id)
            .cloned()
            .ok_or_else(|| cluster_not_found(&cluster_id))?;
        let mut snap =
            snapshot_from_cluster(&cluster, &snap_id, "manual", &req.account_id, &req.region);
        snap.manual_snapshot_retention_period =
            int_param(req, "ManualSnapshotRetentionPeriod").unwrap_or(-1);
        snap.tags = parse_tags(req);
        acct.snapshots.insert(snap_id, snap.clone());
        Ok(xml_resp(
            "CreateClusterSnapshot",
            format!("<Snapshot>{}</Snapshot>", render_snapshot(&snap)),
            &req.request_id,
        ))
    }

    pub(super) fn describe_cluster_snapshots(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let acct = guard.accounts.get(&req.account_id);
        let all: Vec<Snapshot> = match (param(req, "SnapshotIdentifier"), acct) {
            (Some(id), Some(a)) => match a.snapshots.get(&id) {
                Some(s) => vec![s.clone()],
                None => return Err(snapshot_not_found(&id)),
            },
            (Some(id), None) => return Err(snapshot_not_found(&id)),
            (None, Some(a)) => {
                let cluster_filter = param(req, "ClusterIdentifier");
                a.snapshots
                    .values()
                    .filter(|s| {
                        cluster_filter
                            .as_ref()
                            .map(|c| &s.cluster_identifier == c)
                            .unwrap_or(true)
                    })
                    .cloned()
                    .collect()
            }
            (None, None) => Vec::new(),
        };
        let (page, next) = paginate(&all, req);
        let inner: String = page
            .iter()
            .map(|s| format!("<Snapshot>{}</Snapshot>", render_snapshot(s)))
            .collect();
        Ok(xml_resp(
            "DescribeClusterSnapshots",
            format!("{}<Snapshots>{inner}</Snapshots>", render_marker(next)),
            &req.request_id,
        ))
    }

    pub(super) fn delete_cluster_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "SnapshotIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let snap = acct
            .snapshots
            .remove(&id)
            .ok_or_else(|| snapshot_not_found(&id))?;
        Ok(xml_resp(
            "DeleteClusterSnapshot",
            format!("<Snapshot>{}</Snapshot>", render_snapshot(&snap)),
            &req.request_id,
        ))
    }

    pub(super) fn copy_cluster_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let src = param(req, "SourceSnapshotIdentifier").unwrap_or_default();
        let target = param(req, "TargetSnapshotIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.snapshots.contains_key(&target) {
            return Err(snapshot_already_exists(&target));
        }
        let mut snap = acct
            .snapshots
            .get(&src)
            .cloned()
            .ok_or_else(|| snapshot_not_found(&src))?;
        snap.snapshot_identifier = target.clone();
        snap.snapshot_create_time = Utc::now();
        snap.manual_snapshot_retention_period =
            int_param(req, "ManualSnapshotRetentionPeriod").unwrap_or(-1);
        acct.snapshots.insert(target, snap.clone());
        Ok(xml_resp(
            "CopyClusterSnapshot",
            format!("<Snapshot>{}</Snapshot>", render_snapshot(&snap)),
            &req.request_id,
        ))
    }

    pub(super) fn modify_cluster_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "SnapshotIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let snap = acct
            .snapshots
            .get_mut(&id)
            .ok_or_else(|| snapshot_not_found(&id))?;
        if let Some(v) = int_param(req, "ManualSnapshotRetentionPeriod") {
            snap.manual_snapshot_retention_period = v;
        }
        let out = render_snapshot(snap);
        Ok(xml_resp(
            "ModifyClusterSnapshot",
            format!("<Snapshot>{out}</Snapshot>"),
            &req.request_id,
        ))
    }

    pub(super) fn batch_delete_cluster_snapshots(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Identifiers is a list of structs; each carries `.SnapshotIdentifier`.
        let mut deleted = Vec::new();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        // `Identifiers` is a `DeleteClusterSnapshotMessageList`; AWS Query
        // serializes it under the member's `xmlName` wrapper
        // (`DeleteClusterSnapshotMessage`). Accept the plain `.member.` form too.
        for wrapper in [
            "Identifiers.DeleteClusterSnapshotMessage",
            "Identifiers.member",
        ] {
            let before = deleted.len();
            for n in 1..=100 {
                let key = format!("{wrapper}.{n}.SnapshotIdentifier");
                match req.query_params.get(&key) {
                    Some(id) => {
                        if acct.snapshots.remove(id).is_some() {
                            deleted.push(id.clone());
                        }
                    }
                    None => break,
                }
            }
            if deleted.len() > before {
                break;
            }
        }
        let resources: String = deleted.iter().map(|id| tag_elem("String", id)).collect();
        Ok(xml_resp(
            "BatchDeleteClusterSnapshots",
            format!("<Resources>{resources}</Resources><Errors/>"),
            &req.request_id,
        ))
    }

    pub(super) fn batch_modify_cluster_snapshots(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let ids = member_list(req, "SnapshotIdentifierList", "String");
        let retention = int_param(req, "ManualSnapshotRetentionPeriod");
        let mut modified = Vec::new();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        for id in ids {
            if let Some(s) = acct.snapshots.get_mut(&id) {
                if let Some(r) = retention {
                    s.manual_snapshot_retention_period = r;
                }
                modified.push(id);
            }
        }
        let resources: String = modified.iter().map(|id| tag_elem("String", id)).collect();
        Ok(xml_resp(
            "BatchModifyClusterSnapshots",
            format!("<Resources>{resources}</Resources><Errors/>"),
            &req.request_id,
        ))
    }

    pub(super) fn authorize_snapshot_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = param(req, "AccountWithRestoreAccess").unwrap_or_default();
        let id = param(req, "SnapshotIdentifier");
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let snap = match id.as_ref().and_then(|i| acct.snapshots.get_mut(i)) {
            Some(s) => s,
            None => {
                return Err(snapshot_not_found(id.as_deref().unwrap_or("")));
            }
        };
        if !snap.accounts_with_restore_access.contains(&account) {
            snap.accounts_with_restore_access.push(account);
        }
        let out = render_snapshot(snap);
        Ok(xml_resp(
            "AuthorizeSnapshotAccess",
            format!("<Snapshot>{out}</Snapshot>"),
            &req.request_id,
        ))
    }

    pub(super) fn revoke_snapshot_access(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let account = param(req, "AccountWithRestoreAccess").unwrap_or_default();
        let id = param(req, "SnapshotIdentifier");
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        let snap = match id.as_ref().and_then(|i| acct.snapshots.get_mut(i)) {
            Some(s) => s,
            None => return Err(snapshot_not_found(id.as_deref().unwrap_or(""))),
        };
        snap.accounts_with_restore_access.retain(|a| a != &account);
        let out = render_snapshot(snap);
        Ok(xml_resp(
            "RevokeSnapshotAccess",
            format!("<Snapshot>{out}</Snapshot>"),
            &req.request_id,
        ))
    }

    pub(super) fn restore_from_cluster_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = param(req, "ClusterIdentifier").unwrap_or_default();
        let mut guard = self.state.write();
        let acct = guard.account(&req.account_id);
        if acct.clusters.contains_key(&id) {
            return Err(cluster_already_exists(&id));
        }
        // Seed the restored cluster from the source snapshot when available.
        let source = param(req, "SnapshotIdentifier").and_then(|s| acct.snapshots.get(&s).cloned());
        let token = &Uuid::new_v4().simple().to_string()[..12];
        let cluster = Cluster {
            cluster_identifier: id.clone(),
            node_type: param(req, "NodeType")
                .or_else(|| source.as_ref().map(|s| s.node_type.clone()))
                .unwrap_or_else(|| "ra3.xlplus".to_string()),
            cluster_status: "available".to_string(),
            cluster_availability_status: "Available".to_string(),
            master_username: source
                .as_ref()
                .map(|s| s.master_username.clone())
                .unwrap_or_else(|| "awsuser".to_string()),
            db_name: source
                .as_ref()
                .map(|s| s.db_name.clone())
                .unwrap_or_else(|| "dev".to_string()),
            endpoint_address: format!("{id}.{token}.{}.redshift.amazonaws.com", req.region),
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
            cluster_version: source
                .as_ref()
                .map(|s| s.cluster_version.clone())
                .unwrap_or_else(|| "1.0".to_string()),
            allow_version_upgrade: bool_param(req, "AllowVersionUpgrade").unwrap_or(true),
            number_of_nodes: int_param(req, "NumberOfNodes")
                .or_else(|| source.as_ref().map(|s| s.number_of_nodes))
                .unwrap_or(2),
            publicly_accessible: bool_param(req, "PubliclyAccessible").unwrap_or(false),
            encrypted: source.as_ref().map(|s| s.encrypted).unwrap_or(false),
            multi_az: bool_param(req, "MultiAZ").unwrap_or(false),
            cluster_type: "multi-node".to_string(),
            kms_key_id: param(req, "KmsKeyId"),
            enhanced_vpc_routing: bool_param(req, "EnhancedVpcRouting").unwrap_or(false),
            maintenance_track_name: param_or(req, "MaintenanceTrackName", "current"),
            elastic_ip: param(req, "ElasticIp"),
            availability_zone_relocation_status: "disabled".to_string(),
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
            "RestoreFromClusterSnapshot",
            format!("<Cluster>{}</Cluster>", render_cluster(&cluster)),
            &req.request_id,
        ))
    }

    pub(super) fn restore_table_from_cluster_snapshot(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cluster_id = param(req, "ClusterIdentifier").unwrap_or_default();
        let request_id = Uuid::new_v4().simple().to_string();
        let status = TableRestoreStatus {
            table_restore_request_id: request_id.clone(),
            status: "IN_PROGRESS".to_string(),
            cluster_identifier: cluster_id.clone(),
            snapshot_identifier: param(req, "SnapshotIdentifier").unwrap_or_default(),
            source_database_name: param(req, "SourceDatabaseName").unwrap_or_default(),
            source_table_name: param(req, "SourceTableName").unwrap_or_default(),
            new_table_name: param(req, "NewTableName").unwrap_or_default(),
            target_database_name: param(req, "TargetDatabaseName")
                .unwrap_or_else(|| param(req, "SourceDatabaseName").unwrap_or_default()),
            request_time: Utc::now(),
        };
        {
            let mut guard = self.state.write();
            let acct = guard.account(&req.account_id);
            if !acct.clusters.contains_key(&cluster_id) {
                return Err(cluster_not_found(&cluster_id));
            }
            acct.table_restore_status
                .insert(request_id.clone(), status.clone());
        }
        Ok(xml_resp(
            "RestoreTableFromClusterSnapshot",
            format!(
                "<TableRestoreStatus>{}</TableRestoreStatus>",
                render_table_restore(&status)
            ),
            &req.request_id,
        ))
    }

    pub(super) fn describe_table_restore_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let all: Vec<TableRestoreStatus> = guard
            .accounts
            .get(&req.account_id)
            .map(|a| a.table_restore_status.values().cloned().collect())
            .unwrap_or_default();
        let inner: String = all
            .iter()
            .map(|s| {
                format!(
                    "<TableRestoreStatus>{}</TableRestoreStatus>",
                    render_table_restore(s)
                )
            })
            .collect();
        Ok(xml_resp(
            "DescribeTableRestoreStatus",
            format!("<TableRestoreStatusDetails>{inner}</TableRestoreStatusDetails>"),
            &req.request_id,
        ))
    }
}

fn render_table_restore(s: &TableRestoreStatus) -> String {
    format!(
        "<TableRestoreRequestId>{rid}</TableRestoreRequestId>\
         <Status>{status}</Status>\
         <ClusterIdentifier>{cluster}</ClusterIdentifier>\
         <SnapshotIdentifier>{snap}</SnapshotIdentifier>\
         <SourceDatabaseName>{sdb}</SourceDatabaseName>\
         <SourceTableName>{stab}</SourceTableName>\
         <NewTableName>{ntab}</NewTableName>\
         <TargetDatabaseName>{tdb}</TargetDatabaseName>\
         <RequestTime>{time}</RequestTime>",
        rid = xml_escape(&s.table_restore_request_id),
        status = xml_escape(&s.status),
        cluster = xml_escape(&s.cluster_identifier),
        snap = xml_escape(&s.snapshot_identifier),
        sdb = xml_escape(&s.source_database_name),
        stab = xml_escape(&s.source_table_name),
        ntab = xml_escape(&s.new_table_name),
        tdb = xml_escape(&s.target_database_name),
        time = s.request_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
    )
}
