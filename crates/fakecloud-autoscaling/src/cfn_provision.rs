//! CloudFormation-driven container backing for Auto Scaling Groups.
//!
//! When `AWS::AutoScaling::AutoScalingGroup` is provisioned through a
//! CloudFormation stack, the CFN provisioner inserts the group record
//! synchronously (control plane only, `instances` empty so `Ref`/`GetAtt`
//! resolve during provisioning), then asks autoscaling to reconcile it to its
//! desired capacity by launching REAL container-backed EC2 instances — the
//! same instances the direct `CreateAutoScalingGroup` path spawns via
//! `RunInstances`. This mirrors that background reconcile for an
//! already-inserted group, so a CFN-provisioned ASG is genuinely backed by real
//! instances, not phantom metadata.

use std::sync::Arc;

use fakecloud_persistence::SnapshotHook;

use crate::AutoScalingService;
use crate::SharedAutoScalingState;

/// Snapshot hooks the detached CFN capacity reconcile fires after it mutates
/// shared state, so the launched ASG instances (`autoscaling`) and their
/// backing EC2 records (`ec2`) survive a restart. `None` fields make the
/// corresponding persist a no-op (memory mode / unit tests).
#[derive(Clone, Default)]
pub struct CfnReconcilePersistHooks {
    pub autoscaling: Option<SnapshotHook>,
    pub ec2: Option<SnapshotHook>,
}

/// Reconcile a CFN-provisioned Auto Scaling Group to its desired capacity by
/// launching REAL EC2 instances. No-op if the group is gone (e.g. the stack was
/// deleted before reconciliation ran). Intended to be `tokio::spawn`ed by the
/// CloudFormation `CreateStack` drain so stack creation never blocks on a
/// container boot/pull (the #1539/#1730 timeout lesson). With no EC2 backend
/// wired (CI / metadata-only) the group still reaches desired capacity via
/// synthesized instance ids, matching the direct API path.
pub async fn cfn_reconcile_capacity(
    asg_state: SharedAutoScalingState,
    ec2_state: fakecloud_ec2::SharedEc2State,
    ec2_runtime: Option<Arc<fakecloud_ec2::Ec2Runtime>>,
    group_name: String,
    account_id: String,
    region: String,
    persist: CfnReconcilePersistHooks,
) {
    // The EC2 hook is set on the service so `apply_capacity` persists the REAL
    // EC2 instances it launches; the autoscaling hook is fired below to persist
    // the group's `instances` list. Both are required because this reconcile
    // runs DETACHED after `CreateStack`/`UpdateStack` has already serialized the
    // group with `instances=[]`; without them the launched instances (and their
    // EC2 records) vanish on restart (bug-hunt restart-dataloss).
    let svc = AutoScalingService::new(asg_state)
        .with_ec2(ec2_state, ec2_runtime)
        .with_ec2_snapshot_hook(persist.ec2);
    svc.reconcile_group(&account_id, &group_name, &region).await;
    if let Some(hook) = persist.autoscaling {
        hook().await;
    }
}

/// Terminate the REAL EC2 instances launched for a CFN-provisioned Auto Scaling
/// Group when its stack is deleted (or the group is removed by a stack update).
/// Mirrors `cfn_reconcile_capacity` for teardown: the group record has already
/// been removed by the synchronous provisioner delete, so this reaps the
/// orphaned instance containers via the EC2 runtime so a stack delete does not
/// leak real EC2 containers. Intended to be `tokio::spawn`ed by the
/// CloudFormation delete drain. No-op (nothing real to reap) with no EC2
/// backend wired.
pub async fn cfn_terminate_instances(
    asg_state: SharedAutoScalingState,
    ec2_state: fakecloud_ec2::SharedEc2State,
    ec2_runtime: Option<Arc<fakecloud_ec2::Ec2Runtime>>,
    instance_ids: Vec<String>,
    account_id: String,
    region: String,
) {
    let svc = AutoScalingService::new(asg_state).with_ec2(ec2_state, ec2_runtime);
    svc.cfn_terminate_instances(&account_id, &region, &instance_ids)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{AutoScalingAccounts, AutoScalingGroup, AutoScalingSnapshot};
    use fakecloud_persistence::SnapshotStore;
    use parking_lot::{Mutex, RwLock};

    /// Shared cell holding the bytes a capturing snapshot store last saved.
    type Captured = Arc<Mutex<Option<Vec<u8>>>>;

    // A `SnapshotStore` that captures the last-saved bytes in memory so a test
    // can assert what the persist hook serialized (no tempfile needed).
    struct CapturingStore(Captured);

    impl SnapshotStore for CapturingStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.0.lock().clone())
        }
        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            *self.0.lock() = Some(bytes.to_vec());
            Ok(())
        }
    }

    fn capturing() -> (Captured, Arc<dyn SnapshotStore>) {
        let cell: Captured = Arc::new(Mutex::new(None));
        (cell.clone(), Arc::new(CapturingStore(cell)))
    }

    fn group_desired(name: &str, desired: i64) -> AutoScalingGroup {
        AutoScalingGroup {
            name: name.to_string(),
            arn: format!(
                "arn:aws:autoscaling:us-east-1:123456789012:autoScalingGroup:x:autoScalingGroupName/{name}"
            ),
            launch_configuration_name: None,
            launch_template: None,
            min_size: 0,
            max_size: desired,
            desired_capacity: desired,
            default_cooldown: 300,
            availability_zones: vec!["us-east-1a".to_string()],
            vpc_zone_identifier: None,
            health_check_type: "EC2".to_string(),
            health_check_grace_period: 0,
            target_group_arns: Vec::new(),
            load_balancer_names: Vec::new(),
            new_instances_protected_from_scale_in: false,
            created_time: chrono::Utc::now(),
            instances: Vec::new(),
            tags: Vec::new(),
            status: None,
            service_linked_role_arn: String::new(),
        }
    }

    // bug-hunt restart-dataloss #1/#2: a CFN-provisioned ASG is inserted with
    // `instances=[]` and reconciled to capacity by a DETACHED task. That task
    // must persist BOTH the launched ASG instances (autoscaling snapshot) and
    // their backing EC2 records (EC2 snapshot); otherwise the group reloads
    // empty and the EC2 containers leak with no owning state on restart.
    #[tokio::test]
    async fn cfn_reconcile_persists_asg_and_ec2_instances() {
        let account = "123456789012";
        let asg_state: SharedAutoScalingState = Arc::new(RwLock::new(AutoScalingAccounts::new()));
        asg_state
            .write()
            .get_or_create(account)
            .groups
            .insert("g".to_string(), group_desired("g", 2));

        let ec2_state: fakecloud_ec2::SharedEc2State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(account, "us-east-1", ""),
        ));

        // Persist hooks over capturing stores, built exactly as the server wires
        // the autoscaling + EC2 snapshot hooks.
        let (asg_cell, asg_store) = capturing();
        let asg_hook = AutoScalingService::new(asg_state.clone())
            .with_snapshot_store(asg_store)
            .snapshot_hook();
        assert!(asg_hook.is_some(), "autoscaling hook must be built");

        let (ec2_cell, ec2_store) = capturing();
        let ec2_hook = fakecloud_ec2::Ec2Service::with_state(ec2_state.clone())
            .with_snapshot_store(ec2_store)
            .snapshot_hook();
        assert!(ec2_hook.is_some(), "ec2 hook must be built");

        // No EC2 runtime -> metadata-only instances (CI path), still real EC2
        // records in `ec2_state`.
        cfn_reconcile_capacity(
            asg_state.clone(),
            ec2_state.clone(),
            None,
            "g".to_string(),
            account.to_string(),
            "us-east-1".to_string(),
            CfnReconcilePersistHooks {
                autoscaling: asg_hook,
                ec2: ec2_hook,
            },
        )
        .await;

        // In-memory: the group reached desired capacity.
        assert_eq!(
            asg_state.read().accounts[account].groups["g"]
                .instances
                .len(),
            2,
            "reconcile must launch 2 instances"
        );

        // #1: the autoscaling snapshot the detached reconcile fired contains the
        // launched instances (not the empty list CreateStack serialized).
        let asg_bytes = asg_cell
            .lock()
            .clone()
            .expect("autoscaling snapshot written");
        let asg_snap: AutoScalingSnapshot = serde_json::from_slice(&asg_bytes).unwrap();
        let persisted = asg_snap.accounts.expect("multi-account snapshot");
        assert_eq!(
            persisted.accounts[account].groups["g"].instances.len(),
            2,
            "launched ASG instances must survive restart"
        );

        // #2: the EC2 snapshot the reconcile fired contains the backing records,
        // so EC2 boot-recovery has a row to re-drive (no container leak).
        let ec2_bytes = ec2_cell.lock().clone().expect("ec2 snapshot written");
        let ec2_snap: fakecloud_ec2::Ec2Snapshot = serde_json::from_slice(&ec2_bytes).unwrap();
        let ec2_accounts = ec2_snap.accounts.expect("ec2 multi-account snapshot");
        let in_memory = ec2_state.read().default_ref().instances.len();
        assert_eq!(in_memory, 2, "reconcile must create 2 EC2 records");
        assert_eq!(
            ec2_accounts.default_ref().instances.len(),
            in_memory,
            "ASG-launched EC2 records must be persisted (bug #2)"
        );
    }
}
