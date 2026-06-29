//! CloudFormation-driven provisioning for `AWS::EC2::Instance`.
//!
//! The CFN provisioner inserts the instance record synchronously (control plane
//! only, status `pending`, so `Ref`/`GetAtt` resolve to a real `i-...` id and
//! its IPs during provisioning), then asks EC2 to back it with a REAL container
//! in a background task -- the same container the direct `RunInstances` path
//! spawns -- so a CFN-provisioned instance is genuinely backed, not phantom
//! metadata. With no EC2 runtime wired (CI / metadata-only) the instance still
//! reaches `running` via the metadata-only path, matching the direct API.

use std::sync::Arc;

use crate::service::instance::{cfn_boot_instance, cfn_create_instance, cfn_terminate_instance};
use crate::{Ec2Runtime, Ec2Service, SharedEc2State};

pub use crate::service::instance::{CfnInstanceAttrs, CfnInstanceSpec};

/// Synchronously create a control-plane `AWS::EC2::Instance` record and return
/// its Ref/GetAtt attributes (instance id, private/public IP, AZ).
pub fn cfn_create(
    state: SharedEc2State,
    account_id: &str,
    region: &str,
    spec: &CfnInstanceSpec,
) -> CfnInstanceAttrs {
    let svc = Ec2Service::with_state(state);
    cfn_create_instance(&svc, account_id, region, spec)
}

/// Boot the REAL backing container for a CFN-created instance and reconcile it
/// to `running`. No-op if the instance is gone (e.g. the stack was deleted
/// before this ran). Intended to be `tokio::spawn`ed by the CloudFormation
/// `CreateStack` drain.
pub async fn cfn_back_instance(
    state: SharedEc2State,
    runtime: Option<Arc<Ec2Runtime>>,
    account_id: String,
    instance_id: String,
) {
    let svc = Ec2Service::with_state(state).with_runtime(runtime);
    cfn_boot_instance(&svc, &account_id, &instance_id).await;
}

/// Terminate a CFN-created instance and reap its REAL backing container when its
/// stack is deleted. Intended to be `tokio::spawn`ed by the CloudFormation
/// delete drain.
pub async fn cfn_terminate(
    state: SharedEc2State,
    runtime: Option<Arc<Ec2Runtime>>,
    account_id: String,
    region: String,
    instance_id: String,
) {
    let svc = Ec2Service::with_state(state).with_runtime(runtime);
    cfn_terminate_instance(&svc, &account_id, &region, &instance_id).await;
}
