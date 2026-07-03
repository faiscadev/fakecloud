//! AWS Backup restJson1 service handler.
//!
//! Implements the Backup control plane: backup plans + selections + versions,
//! backup vaults (standard / logically-air-gapped / restore-access) with their
//! notifications, access policies and lock configuration, recovery points,
//! backup / copy / restore / scan jobs (progressed synthetically to a terminal
//! state so Describe/List show completed work), frameworks, report plans and
//! report jobs, legal holds, restore-testing plans + selections, tiering
//! configurations, protected resources, tags, and the account-scoped global /
//! region settings.
//!
//! No real backup engine runs: `StartBackupJob` records a job and a synthetic
//! recovery point that `DescribeRecoveryPoint` then resolves, mirroring the
//! eventual-consistency of the real service without moving any bytes.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    framework_arn, legal_hold_arn, plan_arn, recovery_point_arn, report_plan_arn,
    restore_testing_plan_arn, tiering_configuration_arn, vault_arn, PlanRecord, PlanVersion,
    RestoreTestingPlanRecord, RestoreTestingSelectionRecord, SelectionRecord, SharedBackupState,
    TagMap, VaultRecord,
};

/// The resource types AWS Backup can protect (a representative catalogue,
/// returned by `GetSupportedResourceTypes` and used to seed region settings).
const SUPPORTED_RESOURCE_TYPES: &[&str] = &[
    "Aurora",
    "CloudFormation",
    "DocumentDB",
    "DynamoDB",
    "EBS",
    "EC2",
    "EFS",
    "FSx",
    "Neptune",
    "RDS",
    "Redshift",
    "S3",
    "SAP HANA on Amazon EC2",
    "Storage Gateway",
    "Timestream",
    "VirtualMachine",
];

pub const BACKUP_ACTIONS: &[&str] = &[
    "AssociateBackupVaultMpaApprovalTeam",
    "CancelLegalHold",
    "CreateBackupPlan",
    "CreateBackupSelection",
    "CreateBackupVault",
    "CreateFramework",
    "CreateLegalHold",
    "CreateLogicallyAirGappedBackupVault",
    "CreateReportPlan",
    "CreateRestoreAccessBackupVault",
    "CreateRestoreTestingPlan",
    "CreateRestoreTestingSelection",
    "CreateTieringConfiguration",
    "DeleteBackupPlan",
    "DeleteBackupSelection",
    "DeleteBackupVault",
    "DeleteBackupVaultAccessPolicy",
    "DeleteBackupVaultLockConfiguration",
    "DeleteBackupVaultNotifications",
    "DeleteFramework",
    "DeleteRecoveryPoint",
    "DeleteReportPlan",
    "DeleteRestoreTestingPlan",
    "DeleteRestoreTestingSelection",
    "DeleteTieringConfiguration",
    "DescribeBackupJob",
    "DescribeBackupVault",
    "DescribeCopyJob",
    "DescribeFramework",
    "DescribeGlobalSettings",
    "DescribeProtectedResource",
    "DescribeRecoveryPoint",
    "DescribeRegionSettings",
    "DescribeReportJob",
    "DescribeReportPlan",
    "DescribeRestoreJob",
    "DescribeScanJob",
    "DisassociateBackupVaultMpaApprovalTeam",
    "DisassociateRecoveryPoint",
    "DisassociateRecoveryPointFromParent",
    "ExportBackupPlanTemplate",
    "GetBackupPlan",
    "GetBackupPlanFromJSON",
    "GetBackupPlanFromTemplate",
    "GetBackupSelection",
    "GetBackupVaultAccessPolicy",
    "GetBackupVaultNotifications",
    "GetLegalHold",
    "GetPITRMalwareScanResults",
    "GetRecoveryPointIndexDetails",
    "GetRecoveryPointRestoreMetadata",
    "GetRestoreJobMetadata",
    "GetRestoreTestingInferredMetadata",
    "GetRestoreTestingPlan",
    "GetRestoreTestingSelection",
    "GetSupportedResourceTypes",
    "GetTieringConfiguration",
    "ListBackupJobSummaries",
    "ListBackupJobs",
    "ListBackupPlanTemplates",
    "ListBackupPlanVersions",
    "ListBackupPlans",
    "ListBackupSelections",
    "ListBackupVaults",
    "ListCopyJobSummaries",
    "ListCopyJobs",
    "ListFrameworks",
    "ListIndexedRecoveryPoints",
    "ListLegalHolds",
    "ListProtectedResources",
    "ListProtectedResourcesByBackupVault",
    "ListRecoveryPointsByBackupVault",
    "ListRecoveryPointsByLegalHold",
    "ListRecoveryPointsByResource",
    "ListReportJobs",
    "ListReportPlans",
    "ListRestoreAccessBackupVaults",
    "ListRestoreJobSummaries",
    "ListRestoreJobs",
    "ListRestoreJobsByProtectedResource",
    "ListRestoreTestingPlans",
    "ListRestoreTestingSelections",
    "ListScanJobSummaries",
    "ListScanJobs",
    "ListTags",
    "ListTieringConfigurations",
    "PutBackupVaultAccessPolicy",
    "PutBackupVaultLockConfiguration",
    "PutBackupVaultNotifications",
    "PutRestoreValidationResult",
    "RevokeRestoreAccessBackupVault",
    "StartBackupJob",
    "StartCopyJob",
    "StartReportJob",
    "StartRestoreJob",
    "StartScanJob",
    "StopBackupJob",
    "TagResource",
    "UntagResource",
    "UpdateBackupPlan",
    "UpdateFramework",
    "UpdateGlobalSettings",
    "UpdateRecoveryPointIndexSettings",
    "UpdateRecoveryPointLifecycle",
    "UpdateRegionSettings",
    "UpdateReportPlan",
    "UpdateRestoreTestingPlan",
    "UpdateRestoreTestingSelection",
    "UpdateTieringConfiguration",
];

pub struct BackupService {
    state: SharedBackupState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

/// The decoded path labels for a route, in URI order.
type Labels = Vec<String>;

impl BackupService {
    pub fn new(state: SharedBackupState) -> Self {
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

    /// Route a request to `(action, path-labels)` by method + path segments,
    /// preserving internal empty segments so precise slice matches don't
    /// collapse (mirrors the EKS handler's `resolve_action`).
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Labels)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        // Preserve internal AND trailing empty segments (only the leading empty
        // from the leading `/` is dropped) so an omitted/empty path label
        // (e.g. `/audit/frameworks/` for an empty `FrameworkName`) still routes
        // to the intended op, which then rejects the empty identifier with a
        // declared error instead of mis-routing to the collection endpoint.
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        let m = &req.method;
        let d = |s: &str| decode(s);
        let one = |a| Some((a, Vec::new()));
        macro_rules! l {
            ($a:expr, $($x:expr),*) => { Some(($a, vec![$($x),*])) };
        }
        match (m, segs.as_slice()) {
            // ---- backup plans ----
            (&Method::PUT, ["backup", "plans"]) => one("CreateBackupPlan"),
            (&Method::GET, ["backup", "plans"]) => one("ListBackupPlans"),
            (&Method::GET, ["backup", "plans", id]) => l!("GetBackupPlan", d(id)),
            (&Method::POST, ["backup", "plans", id]) => l!("UpdateBackupPlan", d(id)),
            (&Method::DELETE, ["backup", "plans", id]) => l!("DeleteBackupPlan", d(id)),
            (&Method::GET, ["backup", "plans", id, "versions"]) => {
                l!("ListBackupPlanVersions", d(id))
            }
            (&Method::PUT, ["backup", "plans", id, "selections"]) => {
                l!("CreateBackupSelection", d(id))
            }
            (&Method::GET, ["backup", "plans", id, "selections"]) => {
                l!("ListBackupSelections", d(id))
            }
            (&Method::GET, ["backup", "plans", id, "selections", sid]) => {
                l!("GetBackupSelection", d(id), d(sid))
            }
            (&Method::DELETE, ["backup", "plans", id, "selections", sid]) => {
                l!("DeleteBackupSelection", d(id), d(sid))
            }
            (&Method::GET, ["backup", "plans", id, "toTemplate"]) => {
                l!("ExportBackupPlanTemplate", d(id))
            }
            (&Method::POST, ["backup", "template", "json", "toPlan"]) => {
                one("GetBackupPlanFromJSON")
            }
            (&Method::GET, ["backup", "template", "plans"]) => one("ListBackupPlanTemplates"),
            (&Method::GET, ["backup", "template", "plans", tid, "toPlan"]) => {
                l!("GetBackupPlanFromTemplate", d(tid))
            }
            // ---- backup vaults ----
            (&Method::GET, ["backup-vaults"]) => one("ListBackupVaults"),
            (&Method::PUT, ["backup-vaults", name]) => l!("CreateBackupVault", d(name)),
            (&Method::GET, ["backup-vaults", name]) => l!("DescribeBackupVault", d(name)),
            (&Method::DELETE, ["backup-vaults", name]) => l!("DeleteBackupVault", d(name)),
            (&Method::PUT, ["backup-vaults", name, "access-policy"]) => {
                l!("PutBackupVaultAccessPolicy", d(name))
            }
            (&Method::GET, ["backup-vaults", name, "access-policy"]) => {
                l!("GetBackupVaultAccessPolicy", d(name))
            }
            (&Method::DELETE, ["backup-vaults", name, "access-policy"]) => {
                l!("DeleteBackupVaultAccessPolicy", d(name))
            }
            (&Method::PUT, ["backup-vaults", name, "vault-lock"]) => {
                l!("PutBackupVaultLockConfiguration", d(name))
            }
            (&Method::DELETE, ["backup-vaults", name, "vault-lock"]) => {
                l!("DeleteBackupVaultLockConfiguration", d(name))
            }
            (&Method::PUT, ["backup-vaults", name, "notification-configuration"]) => {
                l!("PutBackupVaultNotifications", d(name))
            }
            (&Method::GET, ["backup-vaults", name, "notification-configuration"]) => {
                l!("GetBackupVaultNotifications", d(name))
            }
            (&Method::DELETE, ["backup-vaults", name, "notification-configuration"]) => {
                l!("DeleteBackupVaultNotifications", d(name))
            }
            (&Method::PUT, ["backup-vaults", name, "mpaApprovalTeam"]) => {
                l!("AssociateBackupVaultMpaApprovalTeam", d(name))
            }
            (&Method::POST, ["backup-vaults", name, "mpaApprovalTeam"]) => {
                l!("DisassociateBackupVaultMpaApprovalTeam", d(name))
            }
            (&Method::GET, ["backup-vaults", name, "recovery-points"]) => {
                l!("ListRecoveryPointsByBackupVault", d(name))
            }
            (&Method::GET, ["backup-vaults", name, "recovery-points", rp]) => {
                l!("DescribeRecoveryPoint", d(name), d(rp))
            }
            (&Method::DELETE, ["backup-vaults", name, "recovery-points", rp]) => {
                l!("DeleteRecoveryPoint", d(name), d(rp))
            }
            (&Method::POST, ["backup-vaults", name, "recovery-points", rp]) => {
                l!("UpdateRecoveryPointLifecycle", d(name), d(rp))
            }
            (&Method::GET, ["backup-vaults", name, "recovery-points", rp, "index"]) => {
                l!("GetRecoveryPointIndexDetails", d(name), d(rp))
            }
            (&Method::POST, ["backup-vaults", name, "recovery-points", rp, "index"]) => {
                l!("UpdateRecoveryPointIndexSettings", d(name), d(rp))
            }
            (&Method::GET, ["backup-vaults", name, "recovery-points", rp, "restore-metadata"]) => {
                l!("GetRecoveryPointRestoreMetadata", d(name), d(rp))
            }
            (&Method::POST, ["backup-vaults", name, "recovery-points", rp, "disassociate"]) => {
                l!("DisassociateRecoveryPoint", d(name), d(rp))
            }
            (
                &Method::DELETE,
                ["backup-vaults", name, "recovery-points", rp, "parentAssociation"],
            ) => {
                l!("DisassociateRecoveryPointFromParent", d(name), d(rp))
            }
            (&Method::GET, ["backup-vaults", name, "resources"]) => {
                l!("ListProtectedResourcesByBackupVault", d(name))
            }
            // ---- logically air-gapped / restore-access vaults ----
            (&Method::PUT, ["logically-air-gapped-backup-vaults", name]) => {
                l!("CreateLogicallyAirGappedBackupVault", d(name))
            }
            (
                &Method::GET,
                ["logically-air-gapped-backup-vaults", name, "restore-access-backup-vaults"],
            ) => {
                l!("ListRestoreAccessBackupVaults", d(name))
            }
            (
                &Method::DELETE,
                ["logically-air-gapped-backup-vaults", name, "restore-access-backup-vaults", arn],
            ) => {
                l!("RevokeRestoreAccessBackupVault", d(name), d(arn))
            }
            (&Method::PUT, ["restore-access-backup-vaults"]) => {
                one("CreateRestoreAccessBackupVault")
            }
            // ---- legal holds ----
            (&Method::POST, ["legal-holds"]) => one("CreateLegalHold"),
            (&Method::GET, ["legal-holds"]) => one("ListLegalHolds"),
            (&Method::GET, ["legal-holds", id]) => l!("GetLegalHold", d(id)),
            (&Method::DELETE, ["legal-holds", id]) => l!("CancelLegalHold", d(id)),
            (&Method::GET, ["legal-holds", id, "recovery-points"]) => {
                l!("ListRecoveryPointsByLegalHold", d(id))
            }
            // ---- audit: frameworks / report plans / report jobs / summaries ----
            (&Method::POST, ["audit", "frameworks"]) => one("CreateFramework"),
            (&Method::GET, ["audit", "frameworks"]) => one("ListFrameworks"),
            (&Method::GET, ["audit", "frameworks", name]) => l!("DescribeFramework", d(name)),
            (&Method::PUT, ["audit", "frameworks", name]) => l!("UpdateFramework", d(name)),
            (&Method::DELETE, ["audit", "frameworks", name]) => l!("DeleteFramework", d(name)),
            (&Method::POST, ["audit", "report-plans"]) => one("CreateReportPlan"),
            (&Method::GET, ["audit", "report-plans"]) => one("ListReportPlans"),
            (&Method::GET, ["audit", "report-plans", name]) => l!("DescribeReportPlan", d(name)),
            (&Method::PUT, ["audit", "report-plans", name]) => l!("UpdateReportPlan", d(name)),
            (&Method::DELETE, ["audit", "report-plans", name]) => l!("DeleteReportPlan", d(name)),
            (&Method::GET, ["audit", "report-jobs"]) => one("ListReportJobs"),
            (&Method::GET, ["audit", "report-jobs", id]) => l!("DescribeReportJob", d(id)),
            (&Method::POST, ["audit", "report-jobs", name]) => l!("StartReportJob", d(name)),
            (&Method::GET, ["audit", "backup-job-summaries"]) => one("ListBackupJobSummaries"),
            (&Method::GET, ["audit", "copy-job-summaries"]) => one("ListCopyJobSummaries"),
            (&Method::GET, ["audit", "restore-job-summaries"]) => one("ListRestoreJobSummaries"),
            (&Method::GET, ["audit", "scan-job-summaries"]) => one("ListScanJobSummaries"),
            // ---- backup / copy / restore / scan jobs ----
            (&Method::GET, ["backup-jobs"]) => one("ListBackupJobs"),
            (&Method::PUT, ["backup-jobs"]) => one("StartBackupJob"),
            (&Method::GET, ["backup-jobs", id]) => l!("DescribeBackupJob", d(id)),
            (&Method::POST, ["backup-jobs", id]) => l!("StopBackupJob", d(id)),
            (&Method::GET, ["copy-jobs"]) => one("ListCopyJobs"),
            (&Method::PUT, ["copy-jobs"]) => one("StartCopyJob"),
            (&Method::GET, ["copy-jobs", id]) => l!("DescribeCopyJob", d(id)),
            (&Method::GET, ["restore-jobs"]) => one("ListRestoreJobs"),
            (&Method::PUT, ["restore-jobs"]) => one("StartRestoreJob"),
            (&Method::GET, ["restore-jobs", id]) => l!("DescribeRestoreJob", d(id)),
            (&Method::GET, ["restore-jobs", id, "metadata"]) => l!("GetRestoreJobMetadata", d(id)),
            (&Method::PUT, ["restore-jobs", id, "validations"]) => {
                l!("PutRestoreValidationResult", d(id))
            }
            (&Method::GET, ["scan", "jobs"]) => one("ListScanJobs"),
            (&Method::GET, ["scan", "jobs", id]) => l!("DescribeScanJob", d(id)),
            (&Method::PUT, ["scan", "job"]) => one("StartScanJob"),
            (&Method::GET, ["scan", "pitr-malware-scan-results"]) => {
                one("GetPITRMalwareScanResults")
            }
            // ---- protected resources ----
            (&Method::GET, ["resources"]) => one("ListProtectedResources"),
            (&Method::GET, ["resources", arn]) => l!("DescribeProtectedResource", d(arn)),
            (&Method::GET, ["resources", arn, "recovery-points"]) => {
                l!("ListRecoveryPointsByResource", d(arn))
            }
            (&Method::GET, ["resources", arn, "restore-jobs"]) => {
                l!("ListRestoreJobsByProtectedResource", d(arn))
            }
            // ---- restore testing ----
            (&Method::PUT, ["restore-testing", "plans"]) => one("CreateRestoreTestingPlan"),
            (&Method::GET, ["restore-testing", "plans"]) => one("ListRestoreTestingPlans"),
            (&Method::GET, ["restore-testing", "inferred-metadata"]) => {
                one("GetRestoreTestingInferredMetadata")
            }
            (&Method::GET, ["restore-testing", "plans", name]) => {
                l!("GetRestoreTestingPlan", d(name))
            }
            (&Method::PUT, ["restore-testing", "plans", name]) => {
                l!("UpdateRestoreTestingPlan", d(name))
            }
            (&Method::DELETE, ["restore-testing", "plans", name]) => {
                l!("DeleteRestoreTestingPlan", d(name))
            }
            (&Method::PUT, ["restore-testing", "plans", name, "selections"]) => {
                l!("CreateRestoreTestingSelection", d(name))
            }
            (&Method::GET, ["restore-testing", "plans", name, "selections"]) => {
                l!("ListRestoreTestingSelections", d(name))
            }
            (&Method::GET, ["restore-testing", "plans", name, "selections", sname]) => {
                l!("GetRestoreTestingSelection", d(name), d(sname))
            }
            (&Method::PUT, ["restore-testing", "plans", name, "selections", sname]) => {
                l!("UpdateRestoreTestingSelection", d(name), d(sname))
            }
            (&Method::DELETE, ["restore-testing", "plans", name, "selections", sname]) => {
                l!("DeleteRestoreTestingSelection", d(name), d(sname))
            }
            // ---- tiering configurations ----
            (&Method::PUT, ["tiering-configurations"]) => one("CreateTieringConfiguration"),
            (&Method::GET, ["tiering-configurations"]) => one("ListTieringConfigurations"),
            (&Method::GET, ["tiering-configurations", name]) => {
                l!("GetTieringConfiguration", d(name))
            }
            (&Method::PUT, ["tiering-configurations", name]) => {
                l!("UpdateTieringConfiguration", d(name))
            }
            (&Method::DELETE, ["tiering-configurations", name]) => {
                l!("DeleteTieringConfiguration", d(name))
            }
            // ---- misc ----
            (&Method::GET, ["indexes", "recovery-point"]) => one("ListIndexedRecoveryPoints"),
            (&Method::GET, ["global-settings"]) => one("DescribeGlobalSettings"),
            (&Method::PUT, ["global-settings"]) => one("UpdateGlobalSettings"),
            (&Method::GET, ["account-settings"]) => one("DescribeRegionSettings"),
            (&Method::PUT, ["account-settings"]) => one("UpdateRegionSettings"),
            (&Method::GET, ["supported-resource-types"]) => one("GetSupportedResourceTypes"),
            (&Method::GET, ["tags", arn]) => l!("ListTags", d(arn)),
            (&Method::POST, ["tags", arn]) => l!("TagResource", d(arn)),
            (&Method::POST, ["untag", arn]) => l!("UntagResource", d(arn)),
            _ => None,
        }
    }
}

const MUTATING: &[&str] = &[
    "CreateBackupPlan",
    "CreateBackupSelection",
    "CreateBackupVault",
    "CreateFramework",
    "CreateLegalHold",
    "CreateLogicallyAirGappedBackupVault",
    "CreateReportPlan",
    "CreateRestoreAccessBackupVault",
    "CreateRestoreTestingPlan",
    "CreateRestoreTestingSelection",
    "CreateTieringConfiguration",
    "DeleteBackupPlan",
    "DeleteBackupSelection",
    "DeleteBackupVault",
    "DeleteBackupVaultAccessPolicy",
    "DeleteBackupVaultLockConfiguration",
    "DeleteBackupVaultNotifications",
    "DeleteFramework",
    "DeleteRecoveryPoint",
    "DeleteReportPlan",
    "DeleteRestoreTestingPlan",
    "DeleteRestoreTestingSelection",
    "DeleteTieringConfiguration",
    "AssociateBackupVaultMpaApprovalTeam",
    "DisassociateBackupVaultMpaApprovalTeam",
    "DisassociateRecoveryPoint",
    "DisassociateRecoveryPointFromParent",
    "CancelLegalHold",
    "PutBackupVaultAccessPolicy",
    "PutBackupVaultLockConfiguration",
    "PutBackupVaultNotifications",
    "PutRestoreValidationResult",
    "RevokeRestoreAccessBackupVault",
    "StartBackupJob",
    "StartCopyJob",
    "StartReportJob",
    "StartRestoreJob",
    "StartScanJob",
    "StopBackupJob",
    "TagResource",
    "UntagResource",
    "UpdateBackupPlan",
    "UpdateFramework",
    "UpdateGlobalSettings",
    "UpdateRecoveryPointIndexSettings",
    "UpdateRecoveryPointLifecycle",
    "UpdateRegionSettings",
    "UpdateReportPlan",
    "UpdateRestoreTestingPlan",
    "UpdateRestoreTestingSelection",
    "UpdateTieringConfiguration",
    // Describe settles job/recovery-point lifecycle, which persists.
    "DescribeBackupJob",
    "DescribeCopyJob",
    "DescribeRestoreJob",
];

#[async_trait]
impl AwsService for BackupService {
    fn service_name(&self) -> &str {
        "backup"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, labels) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let result = self.dispatch(action, &labels, &req);

        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        BACKUP_ACTIONS
    }
}

impl BackupService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &self,
        action: &str,
        l: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_query_constraints(action, req)?;
        match action {
            "CreateBackupPlan" => self.create_backup_plan(req),
            "GetBackupPlan" => self.get_backup_plan(req, &l[0]),
            "UpdateBackupPlan" => self.update_backup_plan(req, &l[0]),
            "DeleteBackupPlan" => self.delete_backup_plan(req, &l[0]),
            "ListBackupPlans" => self.list_backup_plans(req),
            "ListBackupPlanVersions" => self.list_backup_plan_versions(req, &l[0]),
            "ListBackupPlanTemplates" => self.list_backup_plan_templates(req),
            "GetBackupPlanFromJSON" => self.get_backup_plan_from_json(req),
            "GetBackupPlanFromTemplate" => self.get_backup_plan_from_template(req, &l[0]),
            "ExportBackupPlanTemplate" => self.export_backup_plan_template(req, &l[0]),
            "CreateBackupSelection" => self.create_backup_selection(req, &l[0]),
            "GetBackupSelection" => self.get_backup_selection(req, &l[0], &l[1]),
            "DeleteBackupSelection" => self.delete_backup_selection(req, &l[0], &l[1]),
            "ListBackupSelections" => self.list_backup_selections(req, &l[0]),
            "CreateBackupVault" => self.create_backup_vault(req, &l[0], "BACKUP_VAULT"),
            "CreateLogicallyAirGappedBackupVault" => self.create_lag_vault(req, &l[0]),
            "CreateRestoreAccessBackupVault" => self.create_restore_access_vault(req),
            "DescribeBackupVault" => self.describe_backup_vault(req, &l[0]),
            "DeleteBackupVault" => self.delete_backup_vault(req, &l[0]),
            "ListBackupVaults" => self.list_backup_vaults(req),
            "ListRestoreAccessBackupVaults" => self.list_restore_access_vaults(req, &l[0]),
            "RevokeRestoreAccessBackupVault" => self.revoke_restore_access_vault(req, &l[0], &l[1]),
            "PutBackupVaultAccessPolicy" => self.put_vault_access_policy(req, &l[0]),
            "GetBackupVaultAccessPolicy" => self.get_vault_access_policy(req, &l[0]),
            "DeleteBackupVaultAccessPolicy" => self.delete_vault_access_policy(req, &l[0]),
            "PutBackupVaultLockConfiguration" => self.put_vault_lock(req, &l[0]),
            "DeleteBackupVaultLockConfiguration" => self.delete_vault_lock(req, &l[0]),
            "PutBackupVaultNotifications" => self.put_vault_notifications(req, &l[0]),
            "GetBackupVaultNotifications" => self.get_vault_notifications(req, &l[0]),
            "DeleteBackupVaultNotifications" => self.delete_vault_notifications(req, &l[0]),
            "AssociateBackupVaultMpaApprovalTeam" => self.associate_mpa(req, &l[0]),
            "DisassociateBackupVaultMpaApprovalTeam" => self.disassociate_mpa(req, &l[0]),
            "DescribeRecoveryPoint" => self.describe_recovery_point(req, &l[0], &l[1]),
            "DeleteRecoveryPoint" => self.delete_recovery_point(req, &l[0], &l[1]),
            "UpdateRecoveryPointLifecycle" => {
                self.update_recovery_point_lifecycle(req, &l[0], &l[1])
            }
            "GetRecoveryPointIndexDetails" => self.get_rp_index_details(req, &l[0], &l[1]),
            "UpdateRecoveryPointIndexSettings" => self.update_rp_index_settings(req, &l[0], &l[1]),
            "GetRecoveryPointRestoreMetadata" => self.get_rp_restore_metadata(req, &l[0], &l[1]),
            "DisassociateRecoveryPoint" => self.disassociate_recovery_point(req, &l[0], &l[1]),
            "DisassociateRecoveryPointFromParent" => self.disassociate_rp_parent(req, &l[0], &l[1]),
            "ListRecoveryPointsByBackupVault" => self.list_rp_by_vault(req, &l[0]),
            "ListRecoveryPointsByResource" => self.list_rp_by_resource(req, &l[0]),
            "ListRecoveryPointsByLegalHold" => self.list_rp_by_legal_hold(req, &l[0]),
            "ListIndexedRecoveryPoints" => self.list_indexed_recovery_points(req),
            "ListProtectedResources" => self.list_protected_resources(req),
            "ListProtectedResourcesByBackupVault" => self.list_protected_by_vault(req, &l[0]),
            "DescribeProtectedResource" => self.describe_protected_resource(req, &l[0]),
            // jobs
            "StartBackupJob" => self.start_backup_job(req),
            "DescribeBackupJob" => self.describe_backup_job(req, &l[0]),
            "StopBackupJob" => self.stop_backup_job(req, &l[0]),
            "ListBackupJobs" => self.list_backup_jobs(req),
            "StartCopyJob" => self.start_copy_job(req),
            "DescribeCopyJob" => self.describe_copy_job(req, &l[0]),
            "ListCopyJobs" => self.list_copy_jobs(req),
            "StartRestoreJob" => self.start_restore_job(req),
            "DescribeRestoreJob" => self.describe_restore_job(req, &l[0]),
            "ListRestoreJobs" => self.list_restore_jobs(req),
            "ListRestoreJobsByProtectedResource" => self.list_restore_jobs_by_resource(req, &l[0]),
            "GetRestoreJobMetadata" => self.get_restore_job_metadata(req, &l[0]),
            "PutRestoreValidationResult" => self.put_restore_validation(req, &l[0]),
            "StartScanJob" => self.start_scan_job(req),
            "DescribeScanJob" => self.describe_scan_job(req, &l[0]),
            "ListScanJobs" => self.list_scan_jobs(req),
            "GetPITRMalwareScanResults" => self.get_pitr_malware_scan_results(req),
            "ListBackupJobSummaries" => self.list_job_summaries(req, "BackupJobSummaries"),
            "ListCopyJobSummaries" => self.list_job_summaries(req, "CopyJobSummaries"),
            "ListRestoreJobSummaries" => self.list_job_summaries(req, "RestoreJobSummaries"),
            "ListScanJobSummaries" => self.list_job_summaries(req, "ScanJobSummaries"),
            // frameworks
            "CreateFramework" => self.create_framework(req),
            "DescribeFramework" => self.describe_framework(req, &l[0]),
            "UpdateFramework" => self.update_framework(req, &l[0]),
            "DeleteFramework" => self.delete_framework(req, &l[0]),
            "ListFrameworks" => self.list_frameworks(req),
            // report plans / jobs
            "CreateReportPlan" => self.create_report_plan(req),
            "DescribeReportPlan" => self.describe_report_plan(req, &l[0]),
            "UpdateReportPlan" => self.update_report_plan(req, &l[0]),
            "DeleteReportPlan" => self.delete_report_plan(req, &l[0]),
            "ListReportPlans" => self.list_report_plans(req),
            "StartReportJob" => self.start_report_job(req, &l[0]),
            "DescribeReportJob" => self.describe_report_job(req, &l[0]),
            "ListReportJobs" => self.list_report_jobs(req),
            // legal holds
            "CreateLegalHold" => self.create_legal_hold(req),
            "GetLegalHold" => self.get_legal_hold(req, &l[0]),
            "CancelLegalHold" => self.cancel_legal_hold(req, &l[0]),
            "ListLegalHolds" => self.list_legal_holds(req),
            // restore testing
            "CreateRestoreTestingPlan" => self.create_rtp(req),
            "GetRestoreTestingPlan" => self.get_rtp(req, &l[0]),
            "UpdateRestoreTestingPlan" => self.update_rtp(req, &l[0]),
            "DeleteRestoreTestingPlan" => self.delete_rtp(req, &l[0]),
            "ListRestoreTestingPlans" => self.list_rtp(req),
            "CreateRestoreTestingSelection" => self.create_rts(req, &l[0]),
            "GetRestoreTestingSelection" => self.get_rts(req, &l[0], &l[1]),
            "UpdateRestoreTestingSelection" => self.update_rts(req, &l[0], &l[1]),
            "DeleteRestoreTestingSelection" => self.delete_rts(req, &l[0], &l[1]),
            "ListRestoreTestingSelections" => self.list_rts(req, &l[0]),
            "GetRestoreTestingInferredMetadata" => self.get_rt_inferred_metadata(req),
            // tiering
            "CreateTieringConfiguration" => self.create_tiering(req),
            "GetTieringConfiguration" => self.get_tiering(req, &l[0]),
            "UpdateTieringConfiguration" => self.update_tiering(req, &l[0]),
            "DeleteTieringConfiguration" => self.delete_tiering(req, &l[0]),
            "ListTieringConfigurations" => self.list_tiering(req),
            // settings + tags
            "DescribeGlobalSettings" => self.describe_global_settings(req),
            "UpdateGlobalSettings" => self.update_global_settings(req),
            "DescribeRegionSettings" => self.describe_region_settings(req),
            "UpdateRegionSettings" => self.update_region_settings(req),
            "GetSupportedResourceTypes" => Ok(ok(json!({
                "ResourceTypes": SUPPORTED_RESOURCE_TYPES,
            }))),
            "ListTags" => self.list_tags(req, &l[0]),
            "TagResource" => self.tag_resource(req, &l[0]),
            "UntagResource" => self.untag_resource(req, &l[0]),
            _ => Err(AwsServiceError::action_not_implemented("backup", action)),
        }
    }
}

include!("handlers.rs");
