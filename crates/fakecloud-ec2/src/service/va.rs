//! Verified Access: instances, trust providers, groups, endpoints, policies,
//! logging configuration, and client config export.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, validate_enum, validate_max_results};
use crate::state::{
    Ec2State, Tag, VerifiedAccessEndpoint, VerifiedAccessGroup, VerifiedAccessInstance,
    VerifiedAccessTrustProvider,
};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

// ---- instances ----

fn instance_xml(i: &VerifiedAccessInstance, tags: &[Tag]) -> String {
    let tps: Vec<String> = i
        .trust_providers
        .iter()
        .map(|id| {
            format!(
                "{}<trustProviderType>user</trustProviderType>",
                ec2_elem("verifiedAccessTrustProviderId", id)
            )
        })
        .collect();
    format!(
        "{}{}{}<creationTime>{}</creationTime><fipsEnabled>false</fipsEnabled>{}",
        ec2_elem("verifiedAccessInstanceId", &i.id),
        ec2_elem("description", &i.description),
        ec2_list("verifiedAccessTrustProviderSet", &tps),
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_verified_access_instance(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("vai");
    let i = VerifiedAccessInstance {
        id: id.clone(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        trust_providers: Vec::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "verified-access-instance",
        );
        let t = state.tags_for(&id).to_vec();
        state.va_instances.insert(id.clone(), i.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVerifiedAccessInstance",
        &req.request_id,
        &format!(
            "<verifiedAccessInstance>{}</verifiedAccessInstance>",
            instance_xml(&i, &tags)
        ),
    ))
}

pub(crate) fn delete_verified_access_instance(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessInstanceId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let i = state
        .va_instances
        .remove(&id)
        .unwrap_or(VerifiedAccessInstance {
            id: id.clone(),
            description: String::new(),
            trust_providers: Vec::new(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVerifiedAccessInstance",
        &req.request_id,
        &format!(
            "<verifiedAccessInstance>{}</verifiedAccessInstance>",
            instance_xml(&i, &tags)
        ),
    ))
}

pub(crate) fn describe_verified_access_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 200)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .va_instances
        .values()
        .map(|i| instance_xml(i, state.tags_for(&i.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVerifiedAccessInstances",
        &req.request_id,
        &ec2_list("verifiedAccessInstanceSet", &items),
    ))
}

pub(crate) fn modify_verified_access_instance(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessInstanceId")?;
    let (i, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let i = match state.va_instances.get_mut(&id) {
            Some(e) => {
                if let Some(d) = req.query_params.get("Description") {
                    e.description = d.clone();
                }
                e.clone()
            }
            None => VerifiedAccessInstance {
                id: id.clone(),
                description: req
                    .query_params
                    .get("Description")
                    .cloned()
                    .unwrap_or_default(),
                trust_providers: Vec::new(),
            },
        };
        let tags = state.tags_for(&id).to_vec();
        (i, tags)
    };
    Ok(Ec2Service::respond(
        "ModifyVerifiedAccessInstance",
        &req.request_id,
        &format!(
            "<verifiedAccessInstance>{}</verifiedAccessInstance>",
            instance_xml(&i, &tags)
        ),
    ))
}

// ---- trust providers ----

fn tp_xml(t: &VerifiedAccessTrustProvider, tags: &[Tag]) -> String {
    format!(
        "{}{}<trustProviderType>{}</trustProviderType>{}<creationTime>{}</creationTime>{}",
        ec2_elem("verifiedAccessTrustProviderId", &t.id),
        ec2_elem("description", &t.description),
        t.trust_provider_type,
        ec2_elem("policyReferenceName", &t.policy_reference_name),
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_verified_access_trust_provider(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let tpt = require(&req.query_params, "TrustProviderType")?;
    let prn = require(&req.query_params, "PolicyReferenceName")?;
    validate_enum(&req.query_params, "TrustProviderType", &["user", "device"])?;
    validate_enum(
        &req.query_params,
        "UserTrustProviderType",
        &["iam-identity-center", "oidc"],
    )?;
    validate_enum(
        &req.query_params,
        "DeviceTrustProviderType",
        &["jamf", "crowdstrike", "jumpcloud"],
    )?;
    let id = gen_id("vatp");
    let t = VerifiedAccessTrustProvider {
        id: id.clone(),
        trust_provider_type: tpt,
        policy_reference_name: prn,
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "verified-access-trust-provider",
        );
        let tg = state.tags_for(&id).to_vec();
        state.va_trust_providers.insert(id.clone(), t.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateVerifiedAccessTrustProvider",
        &req.request_id,
        &format!(
            "<verifiedAccessTrustProvider>{}</verifiedAccessTrustProvider>",
            tp_xml(&t, &tags)
        ),
    ))
}

pub(crate) fn delete_verified_access_trust_provider(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessTrustProviderId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let t = state
        .va_trust_providers
        .remove(&id)
        .unwrap_or(VerifiedAccessTrustProvider {
            id: id.clone(),
            trust_provider_type: "user".to_string(),
            policy_reference_name: "pol".to_string(),
            description: String::new(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVerifiedAccessTrustProvider",
        &req.request_id,
        &format!(
            "<verifiedAccessTrustProvider>{}</verifiedAccessTrustProvider>",
            tp_xml(&t, &tags)
        ),
    ))
}

pub(crate) fn describe_verified_access_trust_providers(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 200)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .va_trust_providers
        .values()
        .map(|t| tp_xml(t, state.tags_for(&t.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVerifiedAccessTrustProviders",
        &req.request_id,
        &ec2_list("verifiedAccessTrustProviderSet", &items),
    ))
}

pub(crate) fn modify_verified_access_trust_provider(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessTrustProviderId")?;
    let (t, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let t = match state.va_trust_providers.get_mut(&id) {
            Some(e) => {
                if let Some(d) = req.query_params.get("Description") {
                    e.description = d.clone();
                }
                e.clone()
            }
            None => VerifiedAccessTrustProvider {
                id: id.clone(),
                trust_provider_type: "user".to_string(),
                policy_reference_name: "pol".to_string(),
                description: req
                    .query_params
                    .get("Description")
                    .cloned()
                    .unwrap_or_default(),
            },
        };
        let tags = state.tags_for(&id).to_vec();
        (t, tags)
    };
    Ok(Ec2Service::respond(
        "ModifyVerifiedAccessTrustProvider",
        &req.request_id,
        &format!(
            "<verifiedAccessTrustProvider>{}</verifiedAccessTrustProvider>",
            tp_xml(&t, &tags)
        ),
    ))
}

pub(crate) fn attach_verified_access_trust_provider(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let inst = require(&req.query_params, "VerifiedAccessInstanceId")?;
    let tp = require(&req.query_params, "VerifiedAccessTrustProviderId")?;
    let (i, t, itags, ttags) = attach_detach(svc, req, &inst, &tp, true);
    Ok(Ec2Service::respond("AttachVerifiedAccessTrustProvider", &req.request_id, &format!("<verifiedAccessTrustProvider>{}</verifiedAccessTrustProvider><verifiedAccessInstance>{}</verifiedAccessInstance>", tp_xml(&t, &ttags), instance_xml(&i, &itags))))
}

pub(crate) fn detach_verified_access_trust_provider(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let inst = require(&req.query_params, "VerifiedAccessInstanceId")?;
    let tp = require(&req.query_params, "VerifiedAccessTrustProviderId")?;
    let (i, t, itags, ttags) = attach_detach(svc, req, &inst, &tp, false);
    Ok(Ec2Service::respond("DetachVerifiedAccessTrustProvider", &req.request_id, &format!("<verifiedAccessTrustProvider>{}</verifiedAccessTrustProvider><verifiedAccessInstance>{}</verifiedAccessInstance>", tp_xml(&t, &ttags), instance_xml(&i, &itags))))
}

#[allow(clippy::type_complexity)]
fn attach_detach(
    svc: &Ec2Service,
    req: &AwsRequest,
    inst: &str,
    tp: &str,
    attach: bool,
) -> (
    VerifiedAccessInstance,
    VerifiedAccessTrustProvider,
    Vec<Tag>,
    Vec<Tag>,
) {
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let i = {
        let entry = state
            .va_instances
            .entry(inst.to_string())
            .or_insert_with(|| VerifiedAccessInstance {
                id: inst.to_string(),
                description: String::new(),
                trust_providers: Vec::new(),
            });
        if attach {
            if !entry.trust_providers.iter().any(|t| t == tp) {
                entry.trust_providers.push(tp.to_string());
            }
        } else {
            entry.trust_providers.retain(|t| t != tp);
        }
        entry.clone()
    };
    let itags = state.tags_for(inst).to_vec();
    let t = state
        .va_trust_providers
        .get(tp)
        .cloned()
        .unwrap_or(VerifiedAccessTrustProvider {
            id: tp.to_string(),
            trust_provider_type: "user".to_string(),
            policy_reference_name: "pol".to_string(),
            description: String::new(),
        });
    let ttags = state.tags_for(tp).to_vec();
    (i, t, itags, ttags)
}

// ---- groups ----

fn group_xml(g: &VerifiedAccessGroup, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}<owner>{}</owner><creationTime>{}</creationTime>{}",
        ec2_elem("verifiedAccessGroupId", &g.id),
        ec2_elem("verifiedAccessInstanceId", &g.instance_id),
        ec2_elem("description", &g.description),
        ec2_elem(
            "verifiedAccessGroupArn",
            &format!(
                "arn:aws:ec2:us-east-1:{owner}:verified-access-group/{}",
                g.id
            )
        ),
        owner,
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_verified_access_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let inst = require(&req.query_params, "VerifiedAccessInstanceId")?;
    let id = gen_id("vagr");
    let g = VerifiedAccessGroup {
        id: id.clone(),
        instance_id: inst,
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "verified-access-group",
        );
        let t = state.tags_for(&id).to_vec();
        state.va_groups.insert(id.clone(), g.clone());
        if let Some(p) = req.query_params.get("PolicyDocument") {
            state.va_group_policies.insert(id.clone(), p.clone());
        }
        t
    };
    Ok(Ec2Service::respond(
        "CreateVerifiedAccessGroup",
        &req.request_id,
        &format!(
            "<verifiedAccessGroup>{}</verifiedAccessGroup>",
            group_xml(&g, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_verified_access_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessGroupId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let g = state.va_groups.remove(&id).unwrap_or(VerifiedAccessGroup {
        id: id.clone(),
        instance_id: "vai-0".to_string(),
        description: String::new(),
    });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    state.va_group_policies.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVerifiedAccessGroup",
        &req.request_id,
        &format!(
            "<verifiedAccessGroup>{}</verifiedAccessGroup>",
            group_xml(&g, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_verified_access_groups(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .va_groups
        .values()
        .map(|g| group_xml(g, state.tags_for(&g.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVerifiedAccessGroups",
        &req.request_id,
        &ec2_list("verifiedAccessGroupSet", &items),
    ))
}

pub(crate) fn modify_verified_access_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessGroupId")?;
    let owner = req.account_id.clone();
    let (g, tags) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let g = match state.va_groups.get_mut(&id) {
            Some(e) => {
                if let Some(d) = req.query_params.get("Description") {
                    e.description = d.clone();
                }
                e.clone()
            }
            None => VerifiedAccessGroup {
                id: id.clone(),
                instance_id: "vai-0".to_string(),
                description: req
                    .query_params
                    .get("Description")
                    .cloned()
                    .unwrap_or_default(),
            },
        };
        let tags = state.tags_for(&id).to_vec();
        (g, tags)
    };
    Ok(Ec2Service::respond(
        "ModifyVerifiedAccessGroup",
        &req.request_id,
        &format!(
            "<verifiedAccessGroup>{}</verifiedAccessGroup>",
            group_xml(&g, &tags, &owner)
        ),
    ))
}

fn policy_body(enabled: bool, doc: &str) -> String {
    let mut b = format!("<policyEnabled>{enabled}</policyEnabled>");
    if !doc.is_empty() {
        b += &ec2_elem("policyDocument", doc);
    }
    b
}

pub(crate) fn get_verified_access_group_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessGroupId")?;
    let accounts = svc.state.read();
    let doc = accounts
        .get(&req.account_id)
        .and_then(|s| s.va_group_policies.get(&id).cloned())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetVerifiedAccessGroupPolicy",
        &req.request_id,
        &policy_body(!doc.is_empty(), &doc),
    ))
}

pub(crate) fn modify_verified_access_group_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessGroupId")?;
    let doc = req
        .query_params
        .get("PolicyDocument")
        .cloned()
        .unwrap_or_default();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .va_group_policies
            .insert(id, doc.clone());
    }
    Ok(Ec2Service::respond(
        "ModifyVerifiedAccessGroupPolicy",
        &req.request_id,
        &policy_body(!doc.is_empty(), &doc),
    ))
}

// ---- endpoints ----

fn endpoint_xml(e: &VerifiedAccessEndpoint, tags: &[Tag]) -> String {
    format!(
        "{}{}{}<endpointType>{}</endpointType><attachmentType>{}</attachmentType><status><code>active</code></status><creationTime>{}</creationTime>{}",
        ec2_elem("verifiedAccessEndpointId", &e.id),
        ec2_elem("verifiedAccessGroupId", &e.group_id),
        ec2_elem("verifiedAccessInstanceId", &e.instance_id),
        e.endpoint_type,
        e.attachment_type,
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_verified_access_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group = require(&req.query_params, "VerifiedAccessGroupId")?;
    let et = require(&req.query_params, "EndpointType")?;
    let at = require(&req.query_params, "AttachmentType")?;
    validate_enum(
        &req.query_params,
        "EndpointType",
        &["load-balancer", "network-interface", "rds", "cidr"],
    )?;
    validate_enum(&req.query_params, "AttachmentType", &["vpc"])?;
    let instance_id = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.va_groups.get(&group).map(|g| g.instance_id.clone()))
            .unwrap_or_else(|| "vai-0".to_string())
    };
    let id = gen_id("vae");
    let e = VerifiedAccessEndpoint {
        id: id.clone(),
        group_id: group,
        instance_id,
        endpoint_type: et,
        attachment_type: at,
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "verified-access-endpoint",
        );
        let t = state.tags_for(&id).to_vec();
        state.va_endpoints.insert(id.clone(), e.clone());
        if let Some(p) = req.query_params.get("PolicyDocument") {
            state.va_endpoint_policies.insert(id.clone(), p.clone());
        }
        t
    };
    Ok(Ec2Service::respond(
        "CreateVerifiedAccessEndpoint",
        &req.request_id,
        &format!(
            "<verifiedAccessEndpoint>{}</verifiedAccessEndpoint>",
            endpoint_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn delete_verified_access_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessEndpointId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let e = state
        .va_endpoints
        .remove(&id)
        .unwrap_or(VerifiedAccessEndpoint {
            id: id.clone(),
            group_id: "vagr-0".to_string(),
            instance_id: "vai-0".to_string(),
            endpoint_type: "load-balancer".to_string(),
            attachment_type: "vpc".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    state.va_endpoint_policies.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVerifiedAccessEndpoint",
        &req.request_id,
        &format!(
            "<verifiedAccessEndpoint>{}</verifiedAccessEndpoint>",
            endpoint_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn describe_verified_access_endpoints(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .va_endpoints
        .values()
        .map(|e| endpoint_xml(e, state.tags_for(&e.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeVerifiedAccessEndpoints",
        &req.request_id,
        &ec2_list("verifiedAccessEndpointSet", &items),
    ))
}

pub(crate) fn modify_verified_access_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessEndpointId")?;
    let (e, tags) = {
        let accounts = svc.state.read();
        let e = accounts
            .get(&req.account_id)
            .and_then(|s| s.va_endpoints.get(&id).cloned())
            .unwrap_or(VerifiedAccessEndpoint {
                id: id.clone(),
                group_id: "vagr-0".to_string(),
                instance_id: "vai-0".to_string(),
                endpoint_type: "load-balancer".to_string(),
                attachment_type: "vpc".to_string(),
            });
        let tags = accounts
            .get(&req.account_id)
            .map(|s| s.tags_for(&id).to_vec())
            .unwrap_or_default();
        (e, tags)
    };
    Ok(Ec2Service::respond(
        "ModifyVerifiedAccessEndpoint",
        &req.request_id,
        &format!(
            "<verifiedAccessEndpoint>{}</verifiedAccessEndpoint>",
            endpoint_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn get_verified_access_endpoint_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessEndpointId")?;
    let accounts = svc.state.read();
    let doc = accounts
        .get(&req.account_id)
        .and_then(|s| s.va_endpoint_policies.get(&id).cloned())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetVerifiedAccessEndpointPolicy",
        &req.request_id,
        &policy_body(!doc.is_empty(), &doc),
    ))
}

pub(crate) fn modify_verified_access_endpoint_policy(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VerifiedAccessEndpointId")?;
    let doc = req
        .query_params
        .get("PolicyDocument")
        .cloned()
        .unwrap_or_default();
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .va_endpoint_policies
            .insert(id, doc.clone());
    }
    Ok(Ec2Service::respond(
        "ModifyVerifiedAccessEndpointPolicy",
        &req.request_id,
        &policy_body(!doc.is_empty(), &doc),
    ))
}

pub(crate) fn get_verified_access_endpoint_targets(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "VerifiedAccessEndpointId")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetVerifiedAccessEndpointTargets",
        &req.request_id,
        &ec2_list("verifiedAccessEndpointTargetSet", &[]),
    ))
}

// ---- logging + export ----

pub(crate) fn describe_verified_access_instance_logging_configurations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 10)?;
    Ok(Ec2Service::respond(
        "DescribeVerifiedAccessInstanceLoggingConfigurations",
        &req.request_id,
        &ec2_list("loggingConfigurationSet", &[]),
    ))
}

pub(crate) fn modify_verified_access_instance_logging_configuration(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let inst = require(&req.query_params, "VerifiedAccessInstanceId")?;
    let body = format!(
        "<loggingConfiguration>{}<accessLogs><logVersion>ocsf-1.0.0-rc.2</logVersion></accessLogs></loggingConfiguration>",
        ec2_elem("verifiedAccessInstanceId", &inst),
    );
    Ok(Ec2Service::respond(
        "ModifyVerifiedAccessInstanceLoggingConfiguration",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn export_verified_access_instance_client_configuration(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let inst = require(&req.query_params, "VerifiedAccessInstanceId")?;
    let body = format!(
        "<version>1</version>{}<region>us-east-1</region>{}",
        ec2_elem("verifiedAccessInstanceId", &inst),
        ec2_list("openVpnConfigurationSet", &[]),
    );
    Ok(Ec2Service::respond(
        "ExportVerifiedAccessInstanceClientConfiguration",
        &req.request_id,
        &body,
    ))
}
