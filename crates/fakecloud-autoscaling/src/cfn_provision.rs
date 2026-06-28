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

use crate::AutoScalingService;
use crate::SharedAutoScalingState;

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
) {
    let svc = AutoScalingService::new(asg_state).with_ec2(ec2_state, ec2_runtime);
    svc.reconcile_group(&account_id, &group_name, &region).await;
}
