//! Per-instance EC2 IMDS behind `http://169.254.169.254` inside an instance's
//! backing container.
//!
//! The EC2 runtime gives every instance container an IMDS proxy (see
//! `fakecloud_ec2::runtime::imds`): a NAT rule sends `169.254.169.254:80` to a
//! proxy in the instance's network namespace, which forwards the request here
//! as `/_fakecloud/ec2/imds/<instance-id>/latest/...`. The instance id in the
//! path is what tells instances apart -- every container reaches fakecloud from
//! an address that says nothing about which instance it is.
//!
//! The answers are the instance's own: its id, AMI, type, private IP and
//! Availability Zone, and credentials for the role of the instance profile
//! associated with it (RunInstances `IamInstanceProfile` or
//! AssociateIamInstanceProfile). An instance without a profile has no
//! `iam/` tree, so those paths are 404, as on EC2. The generic `/latest/*`
//! surface (`imds.rs`) keeps serving the server-wide instance identity.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use fakecloud_ec2::SharedEc2State;
use fakecloud_iam::sts_service::container_creds::ContainerCredentialCache;
use fakecloud_iam::SharedIamState;

use crate::imds::{self, ImdsContext};

/// What the per-instance router needs.
#[derive(Clone)]
pub struct Ec2ImdsState {
    pub ec2: SharedEc2State,
    pub iam: SharedIamState,
    pub cache: Arc<ContainerCredentialCache>,
    pub region: String,
}

/// The instance facts IMDS reports.
#[derive(Debug, Clone)]
struct InstanceFacts {
    account_id: String,
    instance_id: String,
    image_id: String,
    instance_type: String,
    private_ip: String,
    az: String,
    launch_time: String,
    /// Role ARN of the associated instance profile, if any.
    role_arn: Option<String>,
}

pub fn routes(state: Ec2ImdsState) -> Router {
    Router::new()
        .route(
            "/_fakecloud/ec2/imds/{instance_id}/latest/{*rest}",
            any(handle),
        )
        .with_state(Arc::new(state))
}

async fn handle(
    State(state): State<Arc<Ec2ImdsState>>,
    Path((instance_id, rest)): Path<(String, String)>,
    req: Request<Body>,
) -> Response {
    let Some(facts) = instance_facts(&state, &instance_id) else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };
    let path = format!("/latest/{rest}");
    let method = req.method().clone();
    if method == axum::http::Method::GET {
        if let Some(resp) = instance_specific(&facts, &state.region, &path) {
            return resp;
        }
    }
    if path.starts_with("/latest/meta-data/iam") && facts.role_arn.is_none() {
        return (StatusCode::NOT_FOUND, "").into_response();
    }
    let ctx = ImdsContext {
        iam: state.iam.clone(),
        cache: state.cache.clone(),
        account_id: facts.account_id.clone(),
        region: state.region.clone(),
        role_arn: facts.role_arn.clone().unwrap_or_default(),
        instance_id: facts.instance_id.clone(),
    };
    // Re-root the request at `/latest/...` for the shared IMDS paths.
    let (mut parts, body) = req.into_parts();
    let Ok(uri) = path.parse() else {
        return (StatusCode::NOT_FOUND, "").into_response();
    };
    parts.uri = uri;
    let req = Request::from_parts(parts, body);
    imds::serve_imds(&ctx, &req).unwrap_or_else(|| (StatusCode::NOT_FOUND, "").into_response())
}

/// Paths whose answer is a property of this instance rather than of the
/// server-wide IMDS context.
fn instance_specific(facts: &InstanceFacts, region: &str, path: &str) -> Option<Response> {
    let text = |v: &str| imds::text(v.to_string());
    Some(match path {
        "/latest/meta-data/ami-id" => text(&facts.image_id),
        "/latest/meta-data/instance-type" => text(&facts.instance_type),
        "/latest/meta-data/local-ipv4" => text(&facts.private_ip),
        "/latest/meta-data/placement/availability-zone" => text(&facts.az),
        "/latest/meta-data/placement/region" => text(region),
        "/latest/dynamic/instance-identity/document" => Json(serde_json::json!({
            "accountId": facts.account_id,
            "architecture": "x86_64",
            "availabilityZone": facts.az,
            "imageId": facts.image_id,
            "instanceId": facts.instance_id,
            "instanceType": facts.instance_type,
            "privateIp": facts.private_ip,
            "region": region,
            "pendingTime": facts.launch_time,
            "version": "2017-09-30",
        }))
        .into_response(),
        _ => return None,
    })
}

/// Look the instance up across accounts, with the role of its associated
/// instance profile.
fn instance_facts(state: &Ec2ImdsState, instance_id: &str) -> Option<InstanceFacts> {
    let (account_id, mut facts, profile_arn) = {
        let accounts = state.ec2.read();
        let found = accounts.iter().find_map(|(account_id, st)| {
            let inst = st.instances.get(instance_id)?;
            let profile_arn = st
                .iam_instance_profile_associations
                .values()
                .find(|a| a.instance_id == instance_id && a.state == "associated")
                .map(|a| a.iam_instance_profile_arn.clone());
            Some((
                account_id.to_string(),
                InstanceFacts {
                    account_id: account_id.to_string(),
                    instance_id: inst.instance_id.clone(),
                    image_id: inst.image_id.clone(),
                    instance_type: inst.instance_type.clone(),
                    private_ip: inst.private_ip.clone(),
                    az: inst.az.clone(),
                    launch_time: inst.launch_time.clone(),
                    role_arn: None,
                },
                profile_arn,
            ))
        });
        found?
    };
    facts.role_arn = profile_arn.and_then(|arn| role_of_profile(&state.iam, &account_id, &arn));
    Some(facts)
}

/// The role ARN carried by the instance profile `profile_arn`.
fn role_of_profile(iam: &SharedIamState, account_id: &str, profile_arn: &str) -> Option<String> {
    let accounts = iam.read();
    let st = accounts.get(account_id)?;
    let profile = st
        .instance_profiles
        .values()
        .find(|p| p.arn == profile_arn)?;
    let role_name = profile.roles.first()?;
    st.roles.get(role_name).map(|r| r.arn.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    #[test]
    fn instance_specific_paths_report_the_instance() {
        let facts = InstanceFacts {
            account_id: "123456789012".into(),
            instance_id: "i-0abc".into(),
            image_id: "ami-1234".into(),
            instance_type: "t3.small".into(),
            private_ip: "10.0.1.20".into(),
            az: "us-east-1b".into(),
            launch_time: "2026-01-01T00:00:00Z".into(),
            role_arn: None,
        };
        assert!(instance_specific(&facts, "us-east-1", "/latest/meta-data/local-ipv4").is_some());
        assert!(instance_specific(&facts, "us-east-1", "/latest/meta-data/instance-id").is_none());
    }

    #[test]
    fn unknown_instance_has_no_facts() {
        let state = Ec2ImdsState {
            ec2: Arc::new(RwLock::new(MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "",
            ))),
            iam: Arc::new(RwLock::new(MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "",
            ))),
            cache: Arc::new(ContainerCredentialCache::new()),
            region: "us-east-1".into(),
        };
        assert!(instance_facts(&state, "i-missing").is_none());
    }
}
