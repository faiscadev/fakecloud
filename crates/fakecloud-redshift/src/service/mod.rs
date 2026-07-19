//! Amazon Redshift control-plane service (awsQuery protocol).

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    RedshiftAccounts, RedshiftSnapshot, SharedRedshiftState, REDSHIFT_SNAPSHOT_SCHEMA_VERSION,
};

mod access;
mod clusters;
mod groups;
pub(crate) mod helpers;
mod resources;
mod snapshots;
mod subscriptions;

#[cfg(test)]
mod tests;

const SUPPORTED_ACTIONS: &[&str] = &[
    "AcceptReservedNodeExchange",
    "AddPartner",
    "AssociateDataShareConsumer",
    "AuthorizeClusterSecurityGroupIngress",
    "AuthorizeDataShare",
    "AuthorizeEndpointAccess",
    "AuthorizeSnapshotAccess",
    "BatchDeleteClusterSnapshots",
    "BatchModifyClusterSnapshots",
    "CancelResize",
    "CopyClusterSnapshot",
    "CreateAuthenticationProfile",
    "CreateCluster",
    "CreateClusterParameterGroup",
    "CreateClusterSecurityGroup",
    "CreateClusterSnapshot",
    "CreateClusterSubnetGroup",
    "CreateCustomDomainAssociation",
    "CreateEndpointAccess",
    "CreateEventSubscription",
    "CreateHsmClientCertificate",
    "CreateHsmConfiguration",
    "CreateIntegration",
    "CreateRedshiftIdcApplication",
    "CreateScheduledAction",
    "CreateSnapshotCopyGrant",
    "CreateSnapshotSchedule",
    "CreateTags",
    "CreateUsageLimit",
    "DeauthorizeDataShare",
    "DeleteAuthenticationProfile",
    "DeleteCluster",
    "DeleteClusterParameterGroup",
    "DeleteClusterSecurityGroup",
    "DeleteClusterSnapshot",
    "DeleteClusterSubnetGroup",
    "DeleteCustomDomainAssociation",
    "DeleteEndpointAccess",
    "DeleteEventSubscription",
    "DeleteHsmClientCertificate",
    "DeleteHsmConfiguration",
    "DeleteIntegration",
    "DeletePartner",
    "DeleteRedshiftIdcApplication",
    "DeleteResourcePolicy",
    "DeleteScheduledAction",
    "DeleteSnapshotCopyGrant",
    "DeleteSnapshotSchedule",
    "DeleteTags",
    "DeleteUsageLimit",
    "DeregisterNamespace",
    "DescribeAccountAttributes",
    "DescribeAuthenticationProfiles",
    "DescribeClusterDbRevisions",
    "DescribeClusterParameterGroups",
    "DescribeClusterParameters",
    "DescribeClusterSecurityGroups",
    "DescribeClusterSnapshots",
    "DescribeClusterSubnetGroups",
    "DescribeClusterTracks",
    "DescribeClusterVersions",
    "DescribeClusters",
    "DescribeCustomDomainAssociations",
    "DescribeDataShares",
    "DescribeDataSharesForConsumer",
    "DescribeDataSharesForProducer",
    "DescribeDefaultClusterParameters",
    "DescribeEndpointAccess",
    "DescribeEndpointAuthorization",
    "DescribeEventCategories",
    "DescribeEventSubscriptions",
    "DescribeEvents",
    "DescribeHsmClientCertificates",
    "DescribeHsmConfigurations",
    "DescribeInboundIntegrations",
    "DescribeIntegrations",
    "DescribeLoggingStatus",
    "DescribeNodeConfigurationOptions",
    "DescribeOrderableClusterOptions",
    "DescribePartners",
    "DescribeRedshiftIdcApplications",
    "DescribeReservedNodeExchangeStatus",
    "DescribeReservedNodeOfferings",
    "DescribeReservedNodes",
    "DescribeResize",
    "DescribeScheduledActions",
    "DescribeSnapshotCopyGrants",
    "DescribeSnapshotSchedules",
    "DescribeStorage",
    "DescribeTableRestoreStatus",
    "DescribeTags",
    "DescribeUsageLimits",
    "DisableLogging",
    "DisableSnapshotCopy",
    "DisassociateDataShareConsumer",
    "EnableLogging",
    "EnableSnapshotCopy",
    "FailoverPrimaryCompute",
    "GetClusterCredentials",
    "GetClusterCredentialsWithIAM",
    "GetIdentityCenterAuthToken",
    "GetReservedNodeExchangeConfigurationOptions",
    "GetReservedNodeExchangeOfferings",
    "GetResourcePolicy",
    "ListRecommendations",
    "ModifyAquaConfiguration",
    "ModifyAuthenticationProfile",
    "ModifyCluster",
    "ModifyClusterDbRevision",
    "ModifyClusterIamRoles",
    "ModifyClusterMaintenance",
    "ModifyClusterParameterGroup",
    "ModifyClusterSnapshot",
    "ModifyClusterSnapshotSchedule",
    "ModifyClusterSubnetGroup",
    "ModifyCustomDomainAssociation",
    "ModifyEndpointAccess",
    "ModifyEventSubscription",
    "ModifyIntegration",
    "ModifyLakehouseConfiguration",
    "ModifyRedshiftIdcApplication",
    "ModifyScheduledAction",
    "ModifySnapshotCopyRetentionPeriod",
    "ModifySnapshotSchedule",
    "ModifyUsageLimit",
    "PauseCluster",
    "PurchaseReservedNodeOffering",
    "PutResourcePolicy",
    "RebootCluster",
    "RegisterNamespace",
    "RejectDataShare",
    "ResetClusterParameterGroup",
    "ResizeCluster",
    "RestoreFromClusterSnapshot",
    "RestoreTableFromClusterSnapshot",
    "ResumeCluster",
    "RevokeClusterSecurityGroupIngress",
    "RevokeEndpointAccess",
    "RevokeSnapshotAccess",
    "RotateEncryptionKey",
    "UpdatePartnerStatus",
];

/// Amazon Redshift control-plane service. Holds per-account state and, in
/// persistent mode, a [`SnapshotStore`] that every mutating action flushes to.
pub struct RedshiftService {
    state: SharedRedshiftState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl RedshiftService {
    pub fn new(state: SharedRedshiftState) -> Self {
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

    pub fn shared_state(&self) -> SharedRedshiftState {
        Arc::clone(&self.state)
    }

    async fn save_snapshot(&self) {
        save_redshift_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists current state when invoked, or `None` in
    /// memory mode. Mirrors the pattern other services expose for the
    /// CloudFormation provisioner's write-through.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_redshift_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// `true` for any action that mutates persisted state. Reads
    /// (`Describe*`/`Get*`/`List*`) never trigger a snapshot write.
    fn is_mutating(action: &str) -> bool {
        !(action.starts_with("Describe") || action.starts_with("Get") || action.starts_with("List"))
    }

    /// Synchronous entry point for the CloudFormation provisioner. Routes a
    /// single action through the real [`dispatch`](Self::dispatch) handler (the
    /// same code path `handle` runs) so a CFN-provisioned `AWS::Redshift::Cluster`
    /// is created with identical state and validation to the direct API. The CFN
    /// layer fires the registered snapshot hook afterwards for persistence, so
    /// this deliberately skips the async `save_snapshot`.
    pub fn provision_sync(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.dispatch(req)
    }

    fn dispatch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        crate::validation::validate_request(&req.action, req)?;
        match req.action.as_str() {
            "AcceptReservedNodeExchange" => self.accept_reserved_node_exchange(req),
            "AddPartner" => self.add_partner(req),
            "AssociateDataShareConsumer" => self.associate_data_share_consumer(req),
            "AuthorizeClusterSecurityGroupIngress" => {
                self.authorize_cluster_security_group_ingress(req)
            }
            "AuthorizeDataShare" => self.authorize_data_share(req),
            "AuthorizeEndpointAccess" => self.authorize_endpoint_access(req),
            "AuthorizeSnapshotAccess" => self.authorize_snapshot_access(req),
            "BatchDeleteClusterSnapshots" => self.batch_delete_cluster_snapshots(req),
            "BatchModifyClusterSnapshots" => self.batch_modify_cluster_snapshots(req),
            "CancelResize" => self.cancel_resize(req),
            "CopyClusterSnapshot" => self.copy_cluster_snapshot(req),
            "CreateAuthenticationProfile" => self.create_authentication_profile(req),
            "CreateCluster" => self.create_cluster(req),
            "CreateClusterParameterGroup" => self.create_cluster_parameter_group(req),
            "CreateClusterSecurityGroup" => self.create_cluster_security_group(req),
            "CreateClusterSnapshot" => self.create_cluster_snapshot(req),
            "CreateClusterSubnetGroup" => self.create_cluster_subnet_group(req),
            "CreateCustomDomainAssociation" => self.create_custom_domain_association(req),
            "CreateEndpointAccess" => self.create_endpoint_access(req),
            "CreateEventSubscription" => self.create_event_subscription(req),
            "CreateHsmClientCertificate" => self.create_hsm_client_certificate(req),
            "CreateHsmConfiguration" => self.create_hsm_configuration(req),
            "CreateIntegration" => self.create_integration(req),
            "CreateRedshiftIdcApplication" => self.create_redshift_idc_application(req),
            "CreateScheduledAction" => self.create_scheduled_action(req),
            "CreateSnapshotCopyGrant" => self.create_snapshot_copy_grant(req),
            "CreateSnapshotSchedule" => self.create_snapshot_schedule(req),
            "CreateTags" => self.create_tags(req),
            "CreateUsageLimit" => self.create_usage_limit(req),
            "DeauthorizeDataShare" => self.deauthorize_data_share(req),
            "DeleteAuthenticationProfile" => self.delete_authentication_profile(req),
            "DeleteCluster" => self.delete_cluster(req),
            "DeleteClusterParameterGroup" => self.delete_cluster_parameter_group(req),
            "DeleteClusterSecurityGroup" => self.delete_cluster_security_group(req),
            "DeleteClusterSnapshot" => self.delete_cluster_snapshot(req),
            "DeleteClusterSubnetGroup" => self.delete_cluster_subnet_group(req),
            "DeleteCustomDomainAssociation" => self.delete_custom_domain_association(req),
            "DeleteEndpointAccess" => self.delete_endpoint_access(req),
            "DeleteEventSubscription" => self.delete_event_subscription(req),
            "DeleteHsmClientCertificate" => self.delete_hsm_client_certificate(req),
            "DeleteHsmConfiguration" => self.delete_hsm_configuration(req),
            "DeleteIntegration" => self.delete_integration(req),
            "DeletePartner" => self.delete_partner(req),
            "DeleteRedshiftIdcApplication" => self.delete_redshift_idc_application(req),
            "DeleteResourcePolicy" => self.delete_resource_policy(req),
            "DeleteScheduledAction" => self.delete_scheduled_action(req),
            "DeleteSnapshotCopyGrant" => self.delete_snapshot_copy_grant(req),
            "DeleteSnapshotSchedule" => self.delete_snapshot_schedule(req),
            "DeleteTags" => self.delete_tags(req),
            "DeleteUsageLimit" => self.delete_usage_limit(req),
            "DeregisterNamespace" => self.deregister_namespace(req),
            "DescribeAccountAttributes" => self.describe_account_attributes(req),
            "DescribeAuthenticationProfiles" => self.describe_authentication_profiles(req),
            "DescribeClusterDbRevisions" => self.describe_cluster_db_revisions(req),
            "DescribeClusterParameterGroups" => self.describe_cluster_parameter_groups(req),
            "DescribeClusterParameters" => self.describe_cluster_parameters(req),
            "DescribeClusterSecurityGroups" => self.describe_cluster_security_groups(req),
            "DescribeClusterSnapshots" => self.describe_cluster_snapshots(req),
            "DescribeClusterSubnetGroups" => self.describe_cluster_subnet_groups(req),
            "DescribeClusterTracks" => self.describe_cluster_tracks(req),
            "DescribeClusterVersions" => self.describe_cluster_versions(req),
            "DescribeClusters" => self.describe_clusters(req),
            "DescribeCustomDomainAssociations" => self.describe_custom_domain_associations(req),
            "DescribeDataShares" => self.describe_data_shares(req),
            "DescribeDataSharesForConsumer" => self.describe_data_shares_for_consumer(req),
            "DescribeDataSharesForProducer" => self.describe_data_shares_for_producer(req),
            "DescribeDefaultClusterParameters" => self.describe_default_cluster_parameters(req),
            "DescribeEndpointAccess" => self.describe_endpoint_access(req),
            "DescribeEndpointAuthorization" => self.describe_endpoint_authorization(req),
            "DescribeEventCategories" => self.describe_event_categories(req),
            "DescribeEventSubscriptions" => self.describe_event_subscriptions(req),
            "DescribeEvents" => self.describe_events(req),
            "DescribeHsmClientCertificates" => self.describe_hsm_client_certificates(req),
            "DescribeHsmConfigurations" => self.describe_hsm_configurations(req),
            "DescribeInboundIntegrations" => self.describe_inbound_integrations(req),
            "DescribeIntegrations" => self.describe_integrations(req),
            "DescribeLoggingStatus" => self.describe_logging_status(req),
            "DescribeNodeConfigurationOptions" => self.describe_node_configuration_options(req),
            "DescribeOrderableClusterOptions" => self.describe_orderable_cluster_options(req),
            "DescribePartners" => self.describe_partners(req),
            "DescribeRedshiftIdcApplications" => self.describe_redshift_idc_applications(req),
            "DescribeReservedNodeExchangeStatus" => {
                self.describe_reserved_node_exchange_status(req)
            }
            "DescribeReservedNodeOfferings" => self.describe_reserved_node_offerings(req),
            "DescribeReservedNodes" => self.describe_reserved_nodes(req),
            "DescribeResize" => self.describe_resize(req),
            "DescribeScheduledActions" => self.describe_scheduled_actions(req),
            "DescribeSnapshotCopyGrants" => self.describe_snapshot_copy_grants(req),
            "DescribeSnapshotSchedules" => self.describe_snapshot_schedules(req),
            "DescribeStorage" => self.describe_storage(req),
            "DescribeTableRestoreStatus" => self.describe_table_restore_status(req),
            "DescribeTags" => self.describe_tags(req),
            "DescribeUsageLimits" => self.describe_usage_limits(req),
            "DisableLogging" => self.disable_logging(req),
            "DisableSnapshotCopy" => self.disable_snapshot_copy(req),
            "DisassociateDataShareConsumer" => self.disassociate_data_share_consumer(req),
            "EnableLogging" => self.enable_logging(req),
            "EnableSnapshotCopy" => self.enable_snapshot_copy(req),
            "FailoverPrimaryCompute" => self.failover_primary_compute(req),
            "GetClusterCredentials" => self.get_cluster_credentials(req),
            "GetClusterCredentialsWithIAM" => self.get_cluster_credentials_with_iam(req),
            "GetIdentityCenterAuthToken" => self.get_identity_center_auth_token(req),
            "GetReservedNodeExchangeConfigurationOptions" => {
                self.get_reserved_node_exchange_configuration_options(req)
            }
            "GetReservedNodeExchangeOfferings" => self.get_reserved_node_exchange_offerings(req),
            "GetResourcePolicy" => self.get_resource_policy(req),
            "ListRecommendations" => self.list_recommendations(req),
            "ModifyAquaConfiguration" => self.modify_aqua_configuration(req),
            "ModifyAuthenticationProfile" => self.modify_authentication_profile(req),
            "ModifyCluster" => self.modify_cluster(req),
            "ModifyClusterDbRevision" => self.modify_cluster_db_revision(req),
            "ModifyClusterIamRoles" => self.modify_cluster_iam_roles(req),
            "ModifyClusterMaintenance" => self.modify_cluster_maintenance(req),
            "ModifyClusterParameterGroup" => self.modify_cluster_parameter_group(req),
            "ModifyClusterSnapshot" => self.modify_cluster_snapshot(req),
            "ModifyClusterSnapshotSchedule" => self.modify_cluster_snapshot_schedule(req),
            "ModifyClusterSubnetGroup" => self.modify_cluster_subnet_group(req),
            "ModifyCustomDomainAssociation" => self.modify_custom_domain_association(req),
            "ModifyEndpointAccess" => self.modify_endpoint_access(req),
            "ModifyEventSubscription" => self.modify_event_subscription(req),
            "ModifyIntegration" => self.modify_integration(req),
            "ModifyLakehouseConfiguration" => self.modify_lakehouse_configuration(req),
            "ModifyRedshiftIdcApplication" => self.modify_redshift_idc_application(req),
            "ModifyScheduledAction" => self.modify_scheduled_action(req),
            "ModifySnapshotCopyRetentionPeriod" => self.modify_snapshot_copy_retention_period(req),
            "ModifySnapshotSchedule" => self.modify_snapshot_schedule(req),
            "ModifyUsageLimit" => self.modify_usage_limit(req),
            "PauseCluster" => self.pause_cluster(req),
            "PurchaseReservedNodeOffering" => self.purchase_reserved_node_offering(req),
            "PutResourcePolicy" => self.put_resource_policy(req),
            "RebootCluster" => self.reboot_cluster(req),
            "RegisterNamespace" => self.register_namespace(req),
            "RejectDataShare" => self.reject_data_share(req),
            "ResetClusterParameterGroup" => self.reset_cluster_parameter_group(req),
            "ResizeCluster" => self.resize_cluster(req),
            "RestoreFromClusterSnapshot" => self.restore_from_cluster_snapshot(req),
            "RestoreTableFromClusterSnapshot" => self.restore_table_from_cluster_snapshot(req),
            "ResumeCluster" => self.resume_cluster(req),
            "RevokeClusterSecurityGroupIngress" => self.revoke_cluster_security_group_ingress(req),
            "RevokeEndpointAccess" => self.revoke_endpoint_access(req),
            "RevokeSnapshotAccess" => self.revoke_snapshot_access(req),
            "RotateEncryptionKey" => self.rotate_encryption_key(req),
            "UpdatePartnerStatus" => self.update_partner_status(req),
            other => Err(AwsServiceError::action_not_implemented("redshift", other)),
        }
    }
}

/// Persist current Redshift state as a snapshot. Noop in memory mode. Offloads
/// serde + file I/O to the blocking pool, guarded by `lock` so a slower earlier
/// write can't clobber a newer one.
pub async fn save_redshift_snapshot(
    state: &SharedRedshiftState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = RedshiftSnapshot {
        schema_version: REDSHIFT_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write redshift snapshot"),
        Err(err) => tracing::error!(%err, "redshift snapshot task panicked"),
    }
}

impl Default for RedshiftService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(RedshiftAccounts::new())))
    }
}

#[async_trait]
impl AwsService for RedshiftService {
    fn service_name(&self) -> &str {
        "redshift"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = Self::is_mutating(&req.action);
        let result = self.dispatch(&req);
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}
