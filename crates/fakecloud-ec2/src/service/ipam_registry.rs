//! IPAM internet-registry associations and the routing policy registrations
//! (RPKI route origin authorizations) published through them.
//!
//! An association ties an IPAM to one Regional Internet Registry. Registrations
//! hang off it, keyed by CIDR, and every change to them produces a delta: the
//! deltas are the audit trail, so they outlive the registrations they describe.
//!
//! A registration publishes through the association's RPKI service, and
//! `EnableIpamInternetRegistryAssociation` is what establishes that service --
//! the model says "after enabling, you can create Route Origin Authorizations
//! (ROAs)". So an association still in `pending-enable` publishes nothing.
//!
//! `ClientToken` is an idempotency token on every mutating operation here. The
//! association records the tokens it has served, keyed by operation, so a retry
//! replays the delta (or the association) the first call produced instead of
//! failing on the change that call already made. A record carries a
//! fingerprint of what the original call asked for, so a token reused with
//! different parameters is answered with `IdempotentParameterMismatch` rather
//! than a success for a change nobody made, and a timestamp, so the records do
//! not accumulate in the association forever.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_aws::xml::xml_escape;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    ec2_global_arn, filter_value_matches, gen_id, indexed_list, invalid_parameter_value, not_found,
    paginate, parse_filters, require, validate_enum, validate_max_results, Filter,
};
use crate::state::{
    Ec2State, IpamIdempotencyRecord, IpamInternetRegistryAssociation,
    IpamRoutingPolicyRegistration, IpamRoutingPolicyRegistrationDelta, Tag,
};

const RIRS: &[&str] = &["ripe", "apnic", "arin", "lacnic", "nicbr"];

/// `MaxResults` and the raw `NextToken` for the module's paginated reads.
/// `IpamMaxResults` carries `@range 5..1000`.
fn page_params(req: &AwsRequest) -> Result<(Option<usize>, Option<String>), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    // `validate_max_results` only range-checks a value it can parse, so a
    // `MaxResults` that is not a number has to be rejected here: dropping it
    // would leave the read unpaginated and hand back the whole set, which is
    // the opposite of what the caller asked for. A `NextToken` that is not a
    // cursor is rejected the same way.
    let max_results =
        match req.query_params.get("MaxResults").filter(|v| !v.is_empty()) {
            Some(v) => Some(v.parse::<usize>().map_err(|_| {
                invalid_parameter_value(format!("Invalid value '{v}' for MaxResults"))
            })?),
            None => None,
        };
    let next_token = req
        .query_params
        .get("NextToken")
        .filter(|v| !v.is_empty())
        .cloned();
    Ok((max_results, next_token))
}

/// Reject a `NextToken` that is not the offset cursor [`paginate`] hands back,
/// rather than silently restarting the caller from the top.
fn validate_offset_token(token: Option<&str>) -> Result<(), AwsServiceError> {
    match token {
        Some(t) => crate::service_helpers::decode_page_token(t).map(|_| ()),
        None => Ok(()),
    }
}

/// `MaxResults` and `NextToken` for the reads that page by offset.
fn pagination(req: &AwsRequest) -> Result<(Option<usize>, Option<String>), AwsServiceError> {
    let (max_results, next_token) = page_params(req)?;
    validate_offset_token(next_token.as_deref())?;
    Ok((max_results, next_token))
}

/// Page a rendered item list into the operation's set element, plus the
/// `nextToken` every one of these results models.
fn paged_response(
    action: &'static str,
    req: &AwsRequest,
    wrapper: &str,
    items: &[String],
    page: (Option<usize>, Option<String>),
) -> Result<AwsResponse, AwsServiceError> {
    let (max_results, next_token) = page;
    let (items, token) = paginate(items, next_token.as_deref(), max_results)?;
    Ok(page_response(action, req, wrapper, &items, token))
}

/// One already-paged set of rendered items, plus the `nextToken` that fetches
/// what did not fit.
fn page_response(
    action: &'static str,
    req: &AwsRequest,
    wrapper: &str,
    page: &[String],
    token: Option<String>,
) -> AwsResponse {
    Ec2Service::respond(
        action,
        &req.request_id,
        &format!(
            "{}{}",
            ec2_list(wrapper, page),
            token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default()
        ),
    )
}

/// Apply a request's `Filter.N` set: values within a filter are OR'd and the
/// filters themselves are AND'd, which is what AWS does. `candidates` maps a
/// filter name to the values an item offers under it; `None` means the name is
/// not one this operation supports, and an unsupported name matches nothing --
/// the same way the rest of the EC2 describes treat one.
fn matches_filters(filters: &[Filter], candidates: impl Fn(&str) -> Option<Vec<String>>) -> bool {
    filters.iter().all(|f| match candidates(&f.name) {
        Some(values) => f
            .values
            .iter()
            .any(|want| values.iter().any(|have| filter_value_matches(want, have))),
        None => false,
    })
}

fn region_of(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

fn dry_run(req: &AwsRequest) -> bool {
    req.query_params
        .get("DryRun")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

fn client_token(req: &AwsRequest) -> Option<String> {
    req.query_params
        .get("ClientToken")
        .filter(|v| !v.is_empty())
        .cloned()
}

/// How long a served idempotency token keeps replaying. EC2 documents a
/// 24-hour idempotency window for these tokens, so a record older than that
/// can no longer serve a retry and is only weight in the snapshot.
pub(super) const CLIENT_TOKEN_TTL_SECONDS: i64 = 24 * 60 * 60;

/// How many records one association keeps at most. The window alone is not a
/// bound: `ClientToken` is an `@idempotencyToken`, so an SDK fills a fresh
/// UUID in on every call and a tight create/delete loop would leave hundreds
/// of thousands of live records inside the window. The oldest go first, which
/// is the order they stop being useful in.
pub(super) const CLIENT_TOKEN_MAX_RECORDS: usize = 1000;

/// Idempotency records are scoped to the operation, so a token a caller reuses
/// across two different calls cannot replay the other one's result.
fn token_key(action: &str, token: &str) -> String {
    format!("{action}:{token}")
}

/// A fingerprint of everything a mutating request asks for, so a retry can be
/// told apart from a token reused with different parameters. Every parameter
/// counts except the ones that do not describe the change: the protocol's own
/// envelope, the token itself, `DryRun` -- a dry run rehearses the same change,
/// and one carrying a served token replays -- and the SigV4 parameters a
/// presigned URL carries, which differ between two signings of the same call.
///
/// Each pair is length-prefixed, so two different parameter sets cannot
/// flatten to the same string: a `DeltaJson` carrying `&` or `=` would
/// otherwise be able to impersonate another request's parameters and replay
/// its result.
fn request_fingerprint(req: &AwsRequest) -> String {
    let mut pairs: Vec<(&str, &str)> = req
        .query_params
        .iter()
        .filter(|(k, _)| {
            !matches!(k.as_str(), "Action" | "Version" | "ClientToken" | "DryRun")
                && !k.starts_with("X-Amz-")
        })
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    // `query_params` is a hash map, so its iteration order is not stable.
    pairs.sort_unstable();
    let mut out = String::new();
    for (k, v) in pairs {
        out.push_str(&format!("{}:{k}={}:{v};", k.len(), v.len()));
    }
    out
}

/// AWS answers a token reused with different parameters with this rather than
/// the original result: the divergent call asked for something that was never
/// applied, and reporting success for it sends the caller on with a wrong
/// picture of the association.
fn idempotent_parameter_mismatch(token: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "IdempotentParameterMismatch",
        format!("The client token '{token}' was already used with different parameters"),
    )
}

/// Whether a record has fallen out of the idempotency window. A record whose
/// timestamp cannot be read cannot be aged, so it is treated as expired rather
/// than kept forever.
pub(super) fn token_expired(record: &IpamIdempotencyRecord) -> bool {
    match chrono::DateTime::parse_from_rfc3339(&record.recorded_at) {
        Ok(t) => {
            Utc::now()
                .signed_duration_since(t.with_timezone(&Utc))
                .num_seconds()
                > CLIENT_TOKEN_TTL_SECONDS
        }
        Err(_) => true,
    }
}

/// The record an earlier call under this idempotency token left, if the retry
/// asks for the same thing. A retry replays it rather than applying the change
/// a second time (or failing on the state the first call left behind); a token
/// reused with different parameters is not a retry at all.
fn replay_record<'a>(
    a: &'a IpamInternetRegistryAssociation,
    action: &str,
    token: Option<&str>,
    fingerprint: &str,
) -> Result<Option<&'a IpamIdempotencyRecord>, AwsServiceError> {
    let Some(token) = token else {
        return Ok(None);
    };
    let Some(record) = a.client_tokens.get(&token_key(action, token)) else {
        return Ok(None);
    };
    if token_expired(record) {
        return Ok(None);
    }
    if record.fingerprint != fingerprint {
        return Err(idempotent_parameter_mismatch(token));
    }
    Ok(Some(record))
}

/// The delta an earlier call under this idempotency token produced, if the
/// retry asks for the same thing.
fn replay_delta(
    a: &IpamInternetRegistryAssociation,
    action: &str,
    token: Option<&str>,
    fingerprint: &str,
) -> Result<Option<IpamRoutingPolicyRegistrationDelta>, AwsServiceError> {
    let Some(record) = replay_record(a, action, token, fingerprint)? else {
        return Ok(None);
    };
    Ok(a.deltas
        .iter()
        .find(|d| d.delta_id == record.result_id)
        .cloned())
}

fn record_client_token(
    a: &mut IpamInternetRegistryAssociation,
    action: &str,
    token: Option<&str>,
    fingerprint: &str,
    result_id: &str,
) {
    let Some(token) = token else {
        return;
    };
    prune_client_tokens(&mut a.client_tokens);
    a.client_tokens.insert(
        token_key(action, token),
        IpamIdempotencyRecord {
            result_id: result_id.to_string(),
            fingerprint: fingerprint.to_string(),
            recorded_at: now_rfc3339(),
        },
    );
}

/// Drop the records that can no longer serve a retry, so what the resource
/// carries into every snapshot stays bounded: the aged-out ones first, then
/// the oldest survivors while the map is still at the cap. Called before
/// recording a new token, so the new record always fits.
pub(super) fn prune_client_tokens(tokens: &mut BTreeMap<String, IpamIdempotencyRecord>) {
    tokens.retain(|_, r| !token_expired(r));
    while tokens.len() >= CLIENT_TOKEN_MAX_RECORDS {
        let oldest = tokens
            .iter()
            .min_by(|(ak, ar), (bk, br)| ar.recorded_at.cmp(&br.recorded_at).then(ak.cmp(bk)))
            .map(|(k, _)| k.clone());
        match oldest {
            Some(key) => {
                tokens.remove(&key);
            }
            None => break,
        }
    }
}

/// Parse an RFC 3339 time bound, rejecting a malformed one rather than letting
/// a byte comparison silently filter everything out.
fn parse_time_bound(
    req: &AwsRequest,
    key: &str,
) -> Result<Option<chrono::DateTime<Utc>>, AwsServiceError> {
    match req.query_params.get(key).filter(|v| !v.is_empty()) {
        Some(v) => chrono::DateTime::parse_from_rfc3339(v)
            .map(|t| Some(t.with_timezone(&Utc)))
            .map_err(|_| invalid_parameter_value(format!("Invalid value '{v}' for {key}"))),
        None => Ok(None),
    }
}

fn delta_time(d: &IpamRoutingPolicyRegistrationDelta) -> Option<chrono::DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(&d.created_at)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// The prefix length of a CIDR, for comparing a ROA's MaxLength against the
/// prefix it covers.
fn cidr_prefix_len(cidr: &str) -> Option<i64> {
    cidr.split_once('/')
        .and_then(|(_, len)| len.parse::<i64>().ok())
}

pub(super) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn association_not_found(id: &str) -> AwsServiceError {
    not_found("InvalidIpamInternetRegistryAssociationId.NotFound", id)
}

fn get_association<'a>(
    state: &'a mut Ec2State,
    id: &str,
) -> Result<&'a mut IpamInternetRegistryAssociation, AwsServiceError> {
    state
        .ipam_ir_associations
        .get_mut(id)
        .ok_or_else(|| association_not_found(id))
}

/// A registration is a ROA published through the association's RPKI service,
/// and that service only exists once `EnableIpamInternetRegistryAssociation`
/// has been called. Publishing through an association still in
/// `pending-enable` would make that operation decorative.
///
/// Only the writes that publish are gated: `CreateIpamRoutingPolicyRegistration`,
/// `ModifyIpamRoutingPolicyRegistration`, and a batch document that adds. A
/// removal -- `DeleteIpamRoutingPolicyRegistration`, or a batch document that
/// only removes -- is deliberately not gated, because gating it would be a
/// trap with no way out: an association restored from a snapshot taken before
/// registrations were gated still carries them in `pending-enable`, and since
/// `DeleteIpamInternetRegistryAssociation` refuses while registrations remain,
/// gating the removal too would leave the association and its ROAs
/// undeletable. Un-publishing is also never the operation that needs the RPKI
/// service to exist.
fn require_enabled(a: &IpamInternetRegistryAssociation) -> Result<(), AwsServiceError> {
    if a.state == "enable-complete" {
        return Ok(());
    }
    Err(AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "IncorrectState",
        format!(
            "The internet registry association '{}' is in state '{}' and must be enabled before \
             it can publish routing policy registrations",
            a.id, a.state
        ),
    ))
}

fn association_xml(
    a: &IpamInternetRegistryAssociation,
    owner: &str,
    tags: &[Tag],
    region: &str,
) -> String {
    let mut s = String::new();
    s.push_str(&ec2_elem("ownerId", owner));
    s.push_str(&ec2_elem("ipamInternetRegistryAssociationId", &a.id));
    s.push_str(&ec2_elem(
        "ipamInternetRegistryAssociationArn",
        &ec2_global_arn(
            region,
            owner,
            &format!("ipam-internet-registry-association/{}", a.id),
        ),
    ));
    s.push_str(&ec2_elem("ipamId", &a.ipam_id));
    s.push_str(&ec2_elem("ipamRegion", &a.region));
    s.push_str(&ec2_elem("rir", &a.rir));
    s.push_str(&ec2_elem("organizationHandle", &a.organization_handle));
    if let Some(d) = &a.description {
        s.push_str(&ec2_elem("description", d));
    }
    s.push_str(&ec2_elem("state", &a.state));
    if let Some(x) = &a.child_request_xml {
        s.push_str(&ec2_elem("childRequestXml", x));
    }
    if !tags.is_empty() {
        s.push_str(&super::tags::tag_set_xml(tags));
    }
    s
}

fn association_matches(
    a: &IpamInternetRegistryAssociation,
    owner: &str,
    tags: &[Tag],
    filters: &[Filter],
) -> bool {
    matches_filters(filters, |name| match name {
        "ipam-internet-registry-association-id" => Some(vec![a.id.clone()]),
        "ipam-id" => Some(vec![a.ipam_id.clone()]),
        "ipam-region" => Some(vec![a.region.clone()]),
        "rir" => Some(vec![a.rir.clone()]),
        "organization-handle" => Some(vec![a.organization_handle.clone()]),
        "state" => Some(vec![a.state.clone()]),
        "owner-id" => Some(vec![owner.to_string()]),
        "tag-key" => Some(tags.iter().map(|t| t.key.clone()).collect()),
        "tag-value" => Some(tags.iter().map(|t| t.value.clone()).collect()),
        other => other.strip_prefix("tag:").map(|key| {
            tags.iter()
                .filter(|t| t.key == key)
                .map(|t| t.value.clone())
                .collect()
        }),
    })
}

fn delta_xml(d: &IpamRoutingPolicyRegistrationDelta) -> String {
    let mut s = String::new();
    s.push_str(&ec2_elem("deltaId", &d.delta_id));
    s.push_str(&ec2_elem("deltaJson", &d.delta_json));
    s.push_str(&ec2_elem("state", &d.state));
    if let Some(m) = &d.state_message {
        s.push_str(&ec2_elem("stateMessage", m));
    }
    s
}

fn registration_xml(r: &IpamRoutingPolicyRegistration) -> String {
    let mut s = String::new();
    s.push_str(&ec2_elem("cidr", &r.cidr));
    let asns: Vec<String> = r.asns.iter().map(|a| ec2_elem("item", a)).collect();
    if !asns.is_empty() {
        s.push_str(&format!("<asnSet>{}</asnSet>", asns.join("")));
    }
    if let Some(p) = r.permit_more_specific_announcements {
        s.push_str(&format!(
            "<permitMoreSpecificAnnouncements>{p}</permitMoreSpecificAnnouncements>"
        ));
    }
    if let Some(m) = r.max_length {
        s.push_str(&format!("<maxLength>{m}</maxLength>"));
    }
    if let Some(d) = &r.description {
        s.push_str(&ec2_elem("description", d));
    }
    s.push_str(&ec2_elem("latestDeltaId", &r.latest_delta_id));
    s.push_str(&ec2_elem("state", &r.state));
    s
}

/// Record a delta against an association and return its id. Deltas publish
/// immediately here: there is no RIR round trip to wait on.
fn push_delta(a: &mut IpamInternetRegistryAssociation, delta_json: String) -> String {
    let delta = IpamRoutingPolicyRegistrationDelta {
        delta_id: gen_id("ipam-delta"),
        delta_json,
        state: "published".to_string(),
        state_message: None,
        created_at: now_rfc3339(),
    };
    let id = delta.delta_id.clone();
    a.deltas.push(delta);
    id
}

fn delta_response(
    action: &'static str,
    req: &AwsRequest,
    d: &IpamRoutingPolicyRegistrationDelta,
) -> AwsResponse {
    Ec2Service::respond(
        action,
        &req.request_id,
        &format!(
            "<ipamRoutingPolicyRegistrationDelta>{}</ipamRoutingPolicyRegistrationDelta>",
            delta_xml(d)
        ),
    )
}

// ---- associations ----

pub(crate) fn create_ipam_internet_registry_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam_id = require(&req.query_params, "IpamId")?;
    let rir = require(&req.query_params, "Rir")?;
    let organization_handle = require(&req.query_params, "OrganizationHandle")?;
    validate_enum(&req.query_params, "Rir", RIRS)?;
    let token = client_token(req);
    let fingerprint = request_fingerprint(req);

    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    if !state.ipams.contains_key(&ipam_id) {
        return Err(not_found("InvalidIpamId.NotFound", &ipam_id));
    }
    // A DryRun validates the request -- including that the IPAM exists, which
    // is exactly the failure a dry run is for -- and changes nothing.
    if dry_run(req) {
        return Ok(Ec2Service::respond(
            "CreateIpamInternetRegistryAssociation",
            &req.request_id,
            "",
        ));
    }
    // A retry that carries the original token gets the original association
    // back rather than a second one for the same registry. A call that reuses
    // the token for a different IPAM, registry or handle is not a retry, and
    // handing it the original association would report an association it never
    // asked for.
    if let Some(token) = &token {
        if let Some(existing) = state
            .ipam_ir_associations
            .values()
            .find(|a| a.client_token.as_deref() == Some(token.as_str()))
        {
            // An association restored from a snapshot that recorded no
            // fingerprint offers nothing to compare against, so a retry on its
            // token replays rather than failing on evidence never recorded.
            if existing
                .create_fingerprint
                .as_deref()
                .is_some_and(|recorded| recorded != fingerprint)
            {
                return Err(idempotent_parameter_mismatch(token));
            }
            let tags = state.tags.get(&existing.id).cloned().unwrap_or_default();
            return Ok(Ec2Service::respond(
                "CreateIpamInternetRegistryAssociation",
                &req.request_id,
                &format!(
                    "<ipamInternetRegistryAssociation>{}</ipamInternetRegistryAssociation>",
                    association_xml(existing, &owner, &tags, &req.region)
                ),
            ));
        }
    }

    let id = gen_id("ipam-ir-assoc");
    let association = IpamInternetRegistryAssociation {
        id: id.clone(),
        ipam_id,
        region,
        rir,
        organization_handle,
        description: req.query_params.get("Description").cloned(),
        // The association exists but cannot publish until it is enabled
        // against the registry's RPKI service.
        state: "pending-enable".to_string(),
        child_request_xml: None,
        registrations: Default::default(),
        deltas: Vec::new(),
        create_fingerprint: token.as_ref().map(|_| fingerprint),
        client_token: token,
        client_tokens: Default::default(),
    };
    let tags = {
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "ipam-internet-registry-association",
        );
        state.tags.get(&id).cloned().unwrap_or_default()
    };
    state.ipam_ir_associations.insert(id, association.clone());
    Ok(Ec2Service::respond(
        "CreateIpamInternetRegistryAssociation",
        &req.request_id,
        &format!(
            "<ipamInternetRegistryAssociation>{}</ipamInternetRegistryAssociation>",
            association_xml(&association, &owner, &tags, &req.region)
        ),
    ))
}

pub(crate) fn enable_ipam_internet_registry_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let rpki_version = require(&req.query_params, "RpkiVersion")?;
    let service_uri = require(&req.query_params, "ServiceUri")?;
    let child_handle = require(&req.query_params, "ChildHandle")?;
    let parent_handle = require(&req.query_params, "ParentHandle")?;
    let parent_bpki_ta = require(&req.query_params, "ParentBpkiTa")?;
    let token = client_token(req);
    let fingerprint = request_fingerprint(req);

    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let tags = state.tags.get(&id).cloned().unwrap_or_default();
    let a = get_association(state, &id)?;
    // A DryRun validates the request -- including that the association exists
    // -- and changes nothing, matching how the rest of EC2 treats one.
    if dry_run(req) {
        return Ok(Ec2Service::respond(
            "EnableIpamInternetRegistryAssociation",
            &req.request_id,
            "",
        ));
    }
    // A retry under the original token reports the association the first call
    // enabled, leaving the child request it already issued alone. A reuse that
    // carries a different service URI or handle is not a retry: it asks for a
    // child request this association never issued.
    let replaying = replay_record(
        a,
        "EnableIpamInternetRegistryAssociation",
        token.as_deref(),
        &fingerprint,
    )?
    .is_some();
    if !replaying {
        // The child request is the RPKI provisioning document the registry
        // needs; it is what the caller takes to the RIR to finish setup. Every
        // value here is caller-supplied and lands in an attribute value or in
        // element text, so it is entity-escaped as it goes in: the response
        // escapes the blob as a whole, so an unescaped `&` or `"` would come
        // back looking fine and only break when the RIR parses the document.
        a.child_request_xml = Some(format!(
            "<publisher_request version=\"{}\" \
             service_uri=\"{}\" \
             child_handle=\"{}\" \
             parent_handle=\"{}\">\
             <parent_bpki_ta>{}</parent_bpki_ta>\
             </publisher_request>",
            xml_escape(&rpki_version),
            xml_escape(&service_uri),
            xml_escape(&child_handle),
            xml_escape(&parent_handle),
            xml_escape(&parent_bpki_ta),
        ));
        a.state = "enable-complete".to_string();
        let association_id = a.id.clone();
        record_client_token(
            a,
            "EnableIpamInternetRegistryAssociation",
            token.as_deref(),
            &fingerprint,
            &association_id,
        );
    }
    let body = format!(
        "<ipamInternetRegistryAssociation>{}</ipamInternetRegistryAssociation>",
        association_xml(a, &owner, &tags, &req.region)
    );
    Ok(Ec2Service::respond(
        "EnableIpamInternetRegistryAssociation",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_ipam_internet_registry_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    // The model is explicit that the registrations have to be removed before
    // the association can go, and deleting one that still publishes ROAs would
    // orphan them. EC2 reports a delete blocked by what depends on the
    // resource as `DependencyViolation`; the model declares no errors of its
    // own for this operation.
    if !state
        .ipam_ir_associations
        .get(&id)
        .ok_or_else(|| association_not_found(&id))?
        .registrations
        .is_empty()
    {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "DependencyViolation",
            format!(
                "The internet registry association '{id}' still has routing policy \
                 registrations; remove them before deleting it"
            ),
        ));
    }
    // A DryRun validates the request -- including that the association exists
    // and that nothing depends on it -- and changes nothing, matching how the
    // rest of EC2 treats one.
    if dry_run(req) {
        return Ok(Ec2Service::respond(
            "DeleteIpamInternetRegistryAssociation",
            &req.request_id,
            "",
        ));
    }
    let tags = state.tags.get(&id).cloned().unwrap_or_default();
    let mut association = state
        .ipam_ir_associations
        .remove(&id)
        .ok_or_else(|| association_not_found(&id))?;
    // The response reports the association in its terminal state.
    association.state = "delete-complete".to_string();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpamInternetRegistryAssociation",
        &req.request_id,
        &format!(
            "<ipamInternetRegistryAssociation>{}</ipamInternetRegistryAssociation>",
            association_xml(&association, &owner, &tags, &req.region)
        ),
    ))
}

pub(crate) fn describe_ipam_internet_registry_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let page = pagination(req)?;
    let ids = indexed_list(&req.query_params, "IpamInternetRegistryAssociationId");
    let filters = parse_filters(&req.query_params);
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let mut items = Vec::new();
    if let Some(state) = accounts.regional(&req.account_id, &req.region) {
        for (id, a) in &state.ipam_ir_associations {
            if !ids.is_empty() && !ids.contains(id) {
                continue;
            }
            let tags = state.tags.get(id).cloned().unwrap_or_default();
            if !association_matches(a, &owner, &tags, &filters) {
                continue;
            }
            items.push(association_xml(a, &owner, &tags, &req.region));
        }
    }
    paged_response(
        "DescribeIpamInternetRegistryAssociations",
        req,
        "ipamInternetRegistryAssociationSet",
        &items,
        page,
    )
}

// ---- routing policy registrations ----

/// Which of the two single-CIDR registration writes is being applied. Create
/// rejects a CIDR that is already registered and Modify one that is not.
#[derive(Clone, Copy)]
enum RegistrationWrite {
    Create,
    Modify,
}

impl RegistrationWrite {
    /// The operation this write serves: it names the response and scopes the
    /// idempotency records.
    fn action(self) -> &'static str {
        match self {
            Self::Create => "CreateIpamRoutingPolicyRegistration",
            Self::Modify => "ModifyIpamRoutingPolicyRegistration",
        }
    }

    /// How the delta document spells the change.
    fn delta_action(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Modify => "modify",
        }
    }
}

/// What one request says about an optional member. Modify is a partial update,
/// so leaving a member out and clearing it cannot be the same thing: an
/// omitted member keeps the stored value, and a member the caller spells out
/// as empty (ec2Query) or `null` (a delta document) removes it.
#[derive(Clone, Debug, PartialEq)]
enum FieldUpdate<T> {
    Unchanged,
    Clear,
    Set(T),
}

impl<T: Clone> FieldUpdate<T> {
    /// What the member becomes, given what the registration already carries.
    fn resolve(&self, previous: Option<T>) -> Option<T> {
        match self {
            Self::Unchanged => previous,
            Self::Clear => None,
            Self::Set(v) => Some(v.clone()),
        }
    }

    /// The value the request set, if it set one.
    fn value(&self) -> Option<&T> {
        match self {
            Self::Set(v) => Some(v),
            _ => None,
        }
    }
}

/// The fields one registration change carries. The single operations parse
/// them from indexed query parameters and a batch entry parses them from its
/// JSON object, so both reach the same validation and the same writer.
struct RegistrationChange {
    cidr: String,
    asns: Vec<String>,
    permit_more_specific_announcements: FieldUpdate<bool>,
    max_length: FieldUpdate<i64>,
    description: FieldUpdate<String>,
}

/// `IpamRoutingPolicyRegistrationMaxLength` carries `@range 0..48`, and the
/// member documents that it must not be shorter than the CIDR's own prefix
/// length -- a ROA that authorizes less than the prefix it covers announces
/// nothing. Both bounds hold wherever the change came from.
fn validate_change(change: &RegistrationChange) -> Result<(), AwsServiceError> {
    if change.asns.is_empty() {
        return Err(invalid_parameter_value("Asns must not be empty"));
    }
    let Some(&m) = change.max_length.value() else {
        return Ok(());
    };
    if !(0..=48).contains(&m) {
        return Err(invalid_parameter_value(
            "MaxLength must be between 0 and 48",
        ));
    }
    if cidr_prefix_len(&change.cidr).is_some_and(|prefix_len| m < prefix_len) {
        return Err(invalid_parameter_value(format!(
            "MaxLength must be greater than or equal to the prefix length of {}",
            change.cidr
        )));
    }
    Ok(())
}

/// Whether the write is legal against what the association already holds.
/// Kept separate from applying it so a DryRun reaches the same verdict as the
/// real call.
fn check_write(
    a: &IpamInternetRegistryAssociation,
    cidr: &str,
    write: RegistrationWrite,
) -> Result<(), AwsServiceError> {
    match write {
        RegistrationWrite::Create if a.registrations.contains_key(cidr) => {
            Err(invalid_parameter_value(format!(
                "A routing policy registration already exists for {cidr}"
            )))
        }
        RegistrationWrite::Create => Ok(()),
        RegistrationWrite::Modify => require_registered(a, cidr),
    }
}

/// A change to a registration, and a delete of one, both need it to be there:
/// neither has anything to act on otherwise, and reporting success would tell
/// the caller a CIDR was changed or removed that never existed.
fn require_registered(
    a: &IpamInternetRegistryAssociation,
    cidr: &str,
) -> Result<(), AwsServiceError> {
    if a.registrations.contains_key(cidr) {
        return Ok(());
    }
    Err(not_found(
        "InvalidIpamRoutingPolicyRegistration.NotFound",
        cidr,
    ))
}

/// Write one change into the association. A change that lands on a CIDR the
/// association already carries is a partial update: the model requires only
/// `Asns`, so a member the request leaves out keeps the value the registration
/// already carries, and only a member the request clears is removed.
fn apply_write(
    a: &mut IpamInternetRegistryAssociation,
    change: &RegistrationChange,
    delta_id: &str,
) {
    let previous = a.registrations.get(&change.cidr).cloned();
    let creating = previous.is_none();
    a.registrations.insert(
        change.cidr.clone(),
        IpamRoutingPolicyRegistration {
            cidr: change.cidr.clone(),
            asns: change.asns.clone(),
            permit_more_specific_announcements: change.permit_more_specific_announcements.resolve(
                previous
                    .as_ref()
                    .and_then(|p| p.permit_more_specific_announcements),
            ),
            max_length: change
                .max_length
                .resolve(previous.as_ref().and_then(|p| p.max_length)),
            description: change
                .description
                .resolve(previous.as_ref().and_then(|p| p.description.clone())),
            latest_delta_id: delta_id.to_string(),
            state: if creating {
                "create-complete".to_string()
            } else {
                "update-complete".to_string()
            },
        },
    );
}

/// Read one optional member out of an ec2Query request. An omitted member
/// leaves the stored value alone, because Modify is a partial update; a member
/// spelled with an empty value (`Description=`, `MaxLength=`) clears it. A
/// partial update needs some spelling that says "remove this", and the empty
/// value is the one the wire offers -- EC2 already distinguishes a
/// present-but-empty parameter from an absent one elsewhere (`DeleteTags`
/// deletes only the empty-value tag for `Tag.N.Value=`).
fn query_update<T>(
    req: &AwsRequest,
    key: &str,
    parse: impl Fn(&str) -> Result<T, AwsServiceError>,
) -> Result<FieldUpdate<T>, AwsServiceError> {
    match req.query_params.get(key) {
        None => Ok(FieldUpdate::Unchanged),
        Some(v) if v.is_empty() => Ok(FieldUpdate::Clear),
        Some(v) => parse(v).map(FieldUpdate::Set),
    }
}

fn change_from_request(req: &AwsRequest) -> Result<RegistrationChange, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    Ok(RegistrationChange {
        cidr,
        asns: indexed_list(&req.query_params, "Asn"),
        permit_more_specific_announcements: query_update(
            req,
            "PermitMoreSpecificAnnouncements",
            |v| Ok(v.eq_ignore_ascii_case("true")),
        )?,
        max_length: query_update(req, "MaxLength", |v| {
            v.parse::<i64>()
                .map_err(|_| invalid_parameter_value(format!("Invalid value '{v}' for MaxLength")))
        })?,
        description: query_update(req, "Description", |v| Ok(v.to_string()))?,
    })
}

/// Shared body for Create and Modify: both take the same registration fields
/// and report the delta the change produced.
fn upsert_registration(
    svc: &Ec2Service,
    req: &AwsRequest,
    write: RegistrationWrite,
) -> Result<AwsResponse, AwsServiceError> {
    let action = write.action();
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let change = change_from_request(req)?;
    validate_change(&change)?;
    let token = client_token(req);
    let fingerprint = request_fingerprint(req);

    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let a = get_association(state, &id)?;
    require_enabled(a)?;
    // A retry replays the delta the first call produced instead of tripping
    // over the registration that call already wrote. A DryRun carrying a
    // served token replays too: it still changes nothing.
    if let Some(delta) = replay_delta(a, action, token.as_deref(), &fingerprint)? {
        return Ok(delta_response(action, req, &delta));
    }
    check_write(a, &change.cidr, write)?;
    // A DryRun validates the request -- including that the association exists,
    // that it is enabled, and that the CIDR is in the state this operation
    // needs -- and changes nothing, matching how the rest of EC2 treats one.
    if dry_run(req) {
        return Ok(Ec2Service::respond(action, &req.request_id, ""));
    }

    let delta_id = push_delta(a, change_document(&change, write));
    record_client_token(a, action, token.as_deref(), &fingerprint, &delta_id);
    apply_write(a, &change, &delta_id);
    let delta = a.deltas.last().expect("the delta was just pushed").clone();
    Ok(delta_response(action, req, &delta))
}

/// The delta document one single-CIDR change records. It spells the optional
/// members the way a batch document spells them, so the audit trail says what
/// the call asked for: a member left out was left alone, and a `null` one was
/// cleared.
fn change_document(change: &RegistrationChange, write: RegistrationWrite) -> String {
    let mut doc = serde_json::Map::new();
    doc.insert("action".to_string(), write.delta_action().into());
    doc.insert("cidr".to_string(), change.cidr.clone().into());
    doc.insert("asns".to_string(), change.asns.clone().into());
    delta_field(&mut doc, "maxLength", &change.max_length);
    delta_field(&mut doc, "description", &change.description);
    delta_field(
        &mut doc,
        "permitMoreSpecificAnnouncements",
        &change.permit_more_specific_announcements,
    );
    serde_json::Value::Object(doc).to_string()
}

/// Spell one optional member into a delta document: an unchanged member is
/// absent from it, and a cleared one is `null`.
fn delta_field<T: Clone + Into<serde_json::Value>>(
    doc: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    update: &FieldUpdate<T>,
) {
    let value = match update {
        FieldUpdate::Unchanged => return,
        FieldUpdate::Clear => serde_json::Value::Null,
        FieldUpdate::Set(v) => v.clone().into(),
    };
    doc.insert(name.to_string(), value);
}

pub(crate) fn create_ipam_routing_policy_registration(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    upsert_registration(svc, req, RegistrationWrite::Create)
}

pub(crate) fn modify_ipam_routing_policy_registration(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    upsert_registration(svc, req, RegistrationWrite::Modify)
}

pub(crate) fn delete_ipam_routing_policy_registration(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    const ACTION: &str = "DeleteIpamRoutingPolicyRegistration";

    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let cidr = require(&req.query_params, "Cidr")?;
    let token = client_token(req);
    let fingerprint = request_fingerprint(req);
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let a = get_association(state, &id)?;
    // A token reused for a different CIDR is not a retry: replaying the first
    // delete's success would report a CIDR removed that is still registered,
    // and the caller would then meet the `DependencyViolation` that CIDR
    // raises when it deletes the association it was told was empty.
    if let Some(delta) = replay_delta(a, ACTION, token.as_deref(), &fingerprint)? {
        return Ok(delta_response(ACTION, req, &delta));
    }
    // Removing a registration is deliberately not gated on the association
    // being enabled -- see [`require_enabled`].
    //
    // A DryRun validates the request -- including that the association exists
    // and that the CIDR is registered -- and changes nothing, matching how the
    // rest of EC2 treats one.
    require_registered(a, &cidr)?;
    if dry_run(req) {
        return Ok(Ec2Service::respond(ACTION, &req.request_id, ""));
    }
    a.registrations.remove(&cidr);
    let delta_json = serde_json::json!({ "action": "delete", "cidr": cidr }).to_string();
    let delta_id = push_delta(a, delta_json);
    record_client_token(a, ACTION, token.as_deref(), &fingerprint, &delta_id);
    let delta = a.deltas.last().expect("the delta was just pushed").clone();
    Ok(delta_response(ACTION, req, &delta))
}

/// One `add` entry of a batch document. Every field is typed, and a field the
/// caller spelled as the wrong JSON type fails the request rather than being
/// dropped: the delta records the document as published, so an entry that is
/// quietly skipped leaves an audit trail claiming a registration nobody made.
fn batch_addition(entry: &serde_json::Value) -> Result<RegistrationChange, AwsServiceError> {
    let cidr = entry
        .get("cidr")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            invalid_parameter_value("Each DeltaJson 'add' entry must carry a string 'cidr'")
        })?
        .to_string();
    let change = RegistrationChange {
        cidr,
        asns: batch_asns(entry)?,
        permit_more_specific_announcements: batch_field(
            entry,
            "permitMoreSpecificAnnouncements",
            serde_json::Value::as_bool,
            "a boolean",
        )?,
        max_length: batch_field(entry, "maxLength", serde_json::Value::as_i64, "an integer")?,
        description: batch_field(
            entry,
            "description",
            |v| v.as_str().map(str::to_string),
            "a string",
        )?,
    };
    validate_change(&change)?;
    Ok(change)
}

/// Read one optional batch-entry field, rejecting a value of the wrong type.
/// A field the entry omits leaves the stored value alone -- an `add` for a
/// CIDR that is already registered is a partial update -- and an explicit
/// `null` clears it, which is the conventional way a JSON delta document says
/// "remove this".
fn batch_field<T>(
    entry: &serde_json::Value,
    name: &str,
    read: impl Fn(&serde_json::Value) -> Option<T>,
    expected: &str,
) -> Result<FieldUpdate<T>, AwsServiceError> {
    match entry.get(name) {
        None => Ok(FieldUpdate::Unchanged),
        Some(serde_json::Value::Null) => Ok(FieldUpdate::Clear),
        Some(v) => read(v).map(FieldUpdate::Set).ok_or_else(|| {
            invalid_parameter_value(format!("DeltaJson '{name}' must be {expected}"))
        }),
    }
}

/// `AsnList` is a list of strings on the wire, but an ASN is a number and a
/// hand-written delta document spells it as one. Both spellings are accepted;
/// anything else fails rather than yielding a registration that publishes no
/// route origin authorization at all.
fn batch_asns(entry: &serde_json::Value) -> Result<Vec<String>, AwsServiceError> {
    let asns = entry
        .get("asns")
        .ok_or_else(|| invalid_parameter_value("Each DeltaJson 'add' entry must carry 'asns'"))?
        .as_array()
        .ok_or_else(|| invalid_parameter_value("DeltaJson 'asns' must be an array"))?;
    asns.iter()
        .map(|v| match v {
            serde_json::Value::String(s) => Ok(s.clone()),
            serde_json::Value::Number(n) => Ok(n.to_string()),
            _ => Err(invalid_parameter_value(
                "DeltaJson 'asns' entries must be ASNs written as strings or numbers",
            )),
        })
        .collect()
}

/// The CIDRs a batch document removes: either bare strings or objects carrying
/// a `cidr`.
fn batch_removals(doc: &serde_json::Value) -> Result<Vec<String>, AwsServiceError> {
    let Some(entries) = doc.get("remove") else {
        return Ok(Vec::new());
    };
    entries
        .as_array()
        .ok_or_else(|| invalid_parameter_value("DeltaJson 'remove' must be an array"))?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .or_else(|| entry.get("cidr")?.as_str())
                .map(str::to_string)
                .ok_or_else(|| {
                    invalid_parameter_value(
                        "Each DeltaJson 'remove' entry must be a CIDR string or carry a string \
                         'cidr'",
                    )
                })
        })
        .collect()
}

/// A batch of registration changes, described by a JSON document rather than
/// indexed parameters. The whole batch lands as one delta.
pub(crate) fn batch_modify_ipam_routing_policy_registrations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    const ACTION: &str = "BatchModifyIpamRoutingPolicyRegistrations";
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let delta_json = require(&req.query_params, "DeltaJson")?;
    let parsed: serde_json::Value = serde_json::from_str(&delta_json)
        .map_err(|_| invalid_parameter_value("DeltaJson is not valid JSON"))?;
    let token = client_token(req);
    let fingerprint = request_fingerprint(req);

    // The document lists the registrations to add and the CIDRs to remove.
    // Every entry is parsed and validated before anything is written: the
    // delta records the whole document as published, so one mistyped entry has
    // to fail the request rather than leave an audit trail claiming changes
    // that were never applied.
    let additions: Vec<RegistrationChange> = match parsed.get("add") {
        Some(add) => add
            .as_array()
            .ok_or_else(|| invalid_parameter_value("DeltaJson 'add' must be an array"))?
            .iter()
            .map(batch_addition)
            .collect::<Result<_, _>>()?,
        None => Vec::new(),
    };
    let removals = batch_removals(&parsed)?;

    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let a = get_association(state, &id)?;
    // Only a document that publishes needs the RPKI service; one that only
    // removes registrations does not -- see [`require_enabled`].
    if !additions.is_empty() {
        require_enabled(a)?;
    }
    if let Some(delta) = replay_delta(a, ACTION, token.as_deref(), &fingerprint)? {
        return Ok(delta_response(ACTION, req, &delta));
    }
    // The document is checked against the state it would itself leave, in the
    // order it is applied below -- additions first, removals after -- so a
    // document that adds a CIDR and removes it again is self-consistent rather
    // than a not-found against the state it has not been applied to yet. An
    // `add` entry may create or update, so it needs no check of its own, but a
    // `remove` for a CIDR that neither exists nor is added by the document
    // removes nothing and must not be reported as published.
    let mut registered: BTreeSet<&str> = a.registrations.keys().map(String::as_str).collect();
    registered.extend(additions.iter().map(|change| change.cidr.as_str()));
    for cidr in &removals {
        if !registered.remove(cidr.as_str()) {
            return Err(not_found(
                "InvalidIpamRoutingPolicyRegistration.NotFound",
                cidr,
            ));
        }
    }
    // A DryRun validates the request -- including every entry of the document
    // -- and changes nothing, matching how the rest of EC2 treats one.
    if dry_run(req) {
        return Ok(Ec2Service::respond(ACTION, &req.request_id, ""));
    }
    let delta_id = push_delta(a, delta_json.clone());
    record_client_token(a, ACTION, token.as_deref(), &fingerprint, &delta_id);
    for change in &additions {
        apply_write(a, change, &delta_id);
    }
    for cidr in &removals {
        a.registrations.remove(cidr);
    }

    let delta = a.deltas.last().expect("the delta was just pushed").clone();
    Ok(delta_response(ACTION, req, &delta))
}

pub(crate) fn get_ipam_routing_policy_registrations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let page = pagination(req)?;
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let cidr = req.query_params.get("Cidr").filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.ipam_ir_associations.get(&id))
        .ok_or_else(|| association_not_found(&id))?
        .registrations
        .values()
        .filter(|r| cidr.is_none_or(|c| &r.cidr == c))
        .map(registration_xml)
        .collect();
    paged_response(
        "GetIpamRoutingPolicyRegistrations",
        req,
        "ipamRoutingPolicyRegistrationSet",
        &items,
        page,
    )
}

pub(crate) fn get_ipam_routing_policy_registration_deltas(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let (max_results, next_token) = page_params(req)?;
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    validate_enum(
        &req.query_params,
        "ChronologicalOrder",
        &["forward", "reverse"],
    )?;
    let delta_id = req.query_params.get("DeltaId").filter(|v| !v.is_empty());
    let start = parse_time_bound(req, "StartTime")?;
    let end = parse_time_bound(req, "EndTime")?;

    let accounts = svc.state.read();
    let a = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.ipam_ir_associations.get(&id))
        .ok_or_else(|| association_not_found(&id))?;

    let mut deltas: Vec<&IpamRoutingPolicyRegistrationDelta> = a
        .deltas
        .iter()
        .filter(|d| delta_id.is_none_or(|want| &d.delta_id == want))
        // Compare instants, not strings: the stored timestamps carry
        // milliseconds and an SDK omits them when they are zero, so a byte-wise
        // `>=` drops every delta in the same second as the bound.
        .filter(|d| start.is_none_or(|s| delta_time(d).is_none_or(|t| t >= s)))
        .filter(|d| end.is_none_or(|e| delta_time(d).is_none_or(|t| t <= e)))
        .collect();
    // Deltas are stored oldest first; `reverse` reports newest first.
    let reverse = req
        .query_params
        .get("ChronologicalOrder")
        .map(String::as_str)
        == Some("reverse");
    if reverse {
        deltas.reverse();
    }
    // Deltas are appended, so under `reverse` an offset cursor moves: every
    // delta recorded between two pages shifts the index of everything the
    // first page already reported, and the second page repeats items the
    // caller has seen. `reverse` therefore pages by the delta id the next page
    // starts at, which does not move. Forward order is stable under appends
    // and keeps the shared offset cursor.
    let page_start = match next_token.as_deref() {
        None => 0,
        Some(t) if reverse => deltas
            .iter()
            .position(|d| d.delta_id == t)
            .ok_or_else(|| invalid_parameter_value(format!("Invalid value '{t}' for NextToken")))?,
        Some(t) => crate::service_helpers::decode_page_token(t)?.min(deltas.len()),
    };
    let page_end = max_results.map_or(deltas.len(), |n| (page_start + n).min(deltas.len()));
    let token = (page_end < deltas.len()).then(|| {
        if reverse {
            deltas[page_end].delta_id.clone()
        } else {
            crate::service_helpers::encode_page_token(page_end)
        }
    });
    let items: Vec<String> = deltas.into_iter().map(delta_xml).collect();
    Ok(page_response(
        "GetIpamRoutingPolicyRegistrationDeltas",
        req,
        "ipamRoutingPolicyRegistrationDeltaSet",
        &items[page_start..page_end],
        token,
    ))
}

/// The route origin authorizations an association publishes: one per
/// registration and ASN pair, which is the shape a relying party consumes.
pub(crate) fn get_ipam_route_origin_authorizations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let page = pagination(req)?;
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let cidr = req.query_params.get("Cidr").filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let a = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.ipam_ir_associations.get(&id))
        .ok_or_else(|| association_not_found(&id))?;

    let mut items = Vec::new();
    for r in a.registrations.values() {
        if cidr.is_some_and(|c| &r.cidr != c) {
            continue;
        }
        for asn in &r.asns {
            let mut s = ec2_elem("cidr", &r.cidr) + &ec2_elem("asn", asn);
            if let Some(m) = r.max_length {
                s.push_str(&format!("<maxLength>{m}</maxLength>"));
            }
            items.push(s);
        }
    }
    paged_response(
        "GetIpamRouteOriginAuthorizations",
        req,
        "ipamRouteOriginAuthorizationSet",
        &items,
        page,
    )
}

/// Per-ASN and per-CIDR views of what the registry has observed for an
/// association. Both derive from the registrations it publishes.
pub(crate) fn get_ipam_internet_registry_association_asns(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let page = pagination(req)?;
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let filters = parse_filters(&req.query_params);
    let accounts = svc.state.read();
    let a = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.ipam_ir_associations.get(&id))
        .ok_or_else(|| association_not_found(&id))?;

    let mut asns: Vec<&String> = a.registrations.values().flat_map(|r| &r.asns).collect();
    asns.sort();
    asns.dedup();
    let now = now_rfc3339();
    let items: Vec<String> = asns
        .into_iter()
        .filter(|asn| {
            matches_filters(&filters, |name| match name {
                "asn" => Some(vec![asn.to_string()]),
                _ => None,
            })
        })
        .map(|asn| ec2_elem("asn", asn) + &ec2_elem("lastObservedAt", &now))
        .collect();
    paged_response(
        "GetIpamInternetRegistryAssociationAsns",
        req,
        "ipamInternetRegistryAssociationAsnSet",
        &items,
        page,
    )
}

pub(crate) fn get_ipam_internet_registry_association_cidrs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let page = pagination(req)?;
    let id = require(&req.query_params, "IpamInternetRegistryAssociationId")?;
    let filters = parse_filters(&req.query_params);
    let accounts = svc.state.read();
    let a = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.ipam_ir_associations.get(&id))
        .ok_or_else(|| association_not_found(&id))?;

    let now = now_rfc3339();
    let items: Vec<String> = a
        .registrations
        .keys()
        .filter(|cidr| {
            matches_filters(&filters, |name| match name {
                "cidr" => Some(vec![cidr.to_string()]),
                _ => None,
            })
        })
        .map(|cidr| ec2_elem("cidr", cidr) + &ec2_elem("lastObservedAt", &now))
        .collect();
    paged_response(
        "GetIpamInternetRegistryAssociationCidrs",
        req,
        "ipamInternetRegistryAssociationCidrSet",
        &items,
        page,
    )
}

/// Routes a resource discovery has seen in a region. fakecloud runs no BGP
/// collector, so the discovered set is what the account's own registrations
/// advertise there rather than a fabricated view of the internet.
pub(crate) fn get_ipam_discovered_routes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let page = pagination(req)?;
    let discovery_id = require(&req.query_params, "IpamResourceDiscoveryId")?;
    let resource_region = require(&req.query_params, "ResourceRegion")?;
    let filters = parse_filters(&req.query_params);
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let state = accounts
        .regional(&req.account_id, &req.region)
        .ok_or_else(|| not_found("InvalidIpamResourceDiscoveryId.NotFound", &discovery_id))?;
    if !state.ipam_resource_discoveries.contains_key(&discovery_id) {
        return Err(not_found(
            "InvalidIpamResourceDiscoveryId.NotFound",
            &discovery_id,
        ));
    }

    let now = now_rfc3339();
    let mut items = Vec::new();
    for a in state.ipam_ir_associations.values() {
        if a.region != resource_region {
            continue;
        }
        for r in a.registrations.values() {
            let asn = r.asns.first().cloned().unwrap_or_default();
            let keep = matches_filters(&filters, |name| match name {
                "ipam-resource-discovery-id" => Some(vec![discovery_id.clone()]),
                "resource-region" => Some(vec![resource_region.clone()]),
                "resource-owner-id" => Some(vec![owner.clone()]),
                "cidr" => Some(vec![r.cidr.clone()]),
                "asn" => Some(r.asns.clone()),
                "state" => Some(vec!["advertised".to_string()]),
                _ => None,
            });
            if !keep {
                continue;
            }
            items.push(format!(
                "{}{}{}{}{}{}{}",
                ec2_elem("ipamResourceDiscoveryId", &discovery_id),
                ec2_elem("resourceRegion", &resource_region),
                ec2_elem("resourceOwnerId", &owner),
                ec2_elem("cidr", &r.cidr),
                ec2_elem("asn", &asn),
                ec2_elem("state", "advertised"),
                ec2_elem("sampleTime", &now),
            ));
        }
    }
    paged_response(
        "GetIpamDiscoveredRoutes",
        req,
        "ipamDiscoveredRouteSet",
        &items,
        page,
    )
}

/// Route protection findings: a registration whose CIDR is authorized for its
/// ASNs is `valid`; one an association publishes with no ASN at all is
/// `unknown`, which is what an unsigned announcement looks like to RPKI.
pub(crate) fn get_ipam_route_protection_findings(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let (max_results, next_token) = pagination(req)?;
    let ipam_id = require(&req.query_params, "IpamId")?;
    let filters = parse_filters(&req.query_params);
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let state = accounts
        .regional(&req.account_id, &req.region)
        .ok_or_else(|| not_found("InvalidIpamId.NotFound", &ipam_id))?;
    if !state.ipams.contains_key(&ipam_id) {
        return Err(not_found("InvalidIpamId.NotFound", &ipam_id));
    }

    let now = now_rfc3339();
    let mut items = Vec::new();
    for a in state.ipam_ir_associations.values() {
        if a.ipam_id != ipam_id {
            continue;
        }
        for r in a.registrations.values() {
            let asn = r.asns.first().cloned().unwrap_or_default();
            // `IpamRpkiStrength` is `strict | permissive`. A registration that
            // names its origin ASNs authorizes exactly those, which is the
            // strict posture; one with none authorizes nothing specific.
            let (status, strength) = if r.asns.is_empty() {
                ("unknown", "permissive")
            } else {
                ("valid", "strict")
            };
            let keep = matches_filters(&filters, |name| match name {
                "resource-owner-id" => Some(vec![owner.clone()]),
                "resource-region" => Some(vec![a.region.clone()]),
                "cidr" => Some(vec![r.cidr.clone()]),
                "asn" => Some(r.asns.clone()),
                "rpki-status" => Some(vec![status.to_string()]),
                "rpki-strength" => Some(vec![strength.to_string()]),
                _ => None,
            });
            if !keep {
                continue;
            }
            // A finding's `roaSet` holds `IpamRouteOriginAuthorization`, whose
            // prefix member is `prefix`. The `cidr` spelling belongs to
            // `IpamRouteOriginAuthorizationInfo`, the shape
            // GetIpamRouteOriginAuthorizations returns -- emitting it here
            // makes an SDK read the prefix as absent.
            let roas: Vec<String> = r
                .asns
                .iter()
                .map(|asn| {
                    let mut s = ec2_elem("asn", asn) + &ec2_elem("prefix", &r.cidr);
                    if let Some(m) = r.max_length {
                        s.push_str(&format!("<maxLength>{m}</maxLength>"));
                    }
                    s
                })
                .collect();
            let mut finding = format!(
                "{}{}{}{}{}{}{}",
                ec2_elem("resourceOwnerId", &owner),
                ec2_elem("resourceRegion", &a.region),
                ec2_elem("cidr", &r.cidr),
                ec2_elem("asn", &asn),
                ec2_elem("rpkiStatus", status),
                ec2_elem("rpkiStrength", strength),
                ec2_elem("sampleTime", &now),
            );
            if !roas.is_empty() {
                finding.push_str(&ec2_list("roaSet", &roas));
            }
            items.push(finding);
        }
    }
    let (page, token) = paginate(&items, next_token.as_deref(), max_results)?;
    Ok(Ec2Service::respond(
        "GetIpamRouteProtectionFindings",
        &req.request_id,
        &format!(
            "{}{}{}",
            ec2_elem("ipamId", &ipam_id),
            ec2_list("routeProtectionFindingSet", &page),
            token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default()
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    /// Register an IPAM directly so an association has something to attach to.
    fn seed_ipam(svc: &Ec2Service) {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut("000000000000", "us-east-1");
        state.ipams.insert(
            "ipam-1".to_string(),
            crate::state::Ipam {
                id: "ipam-1".to_string(),
                public_scope_id: "ipam-scope-pub".to_string(),
                private_scope_id: "ipam-scope-priv".to_string(),
                tier: "advanced".to_string(),
                description: String::new(),
            },
        );
    }

    /// `nicbr` (NIC.br, Brazil's national registry) is a `Rir` the model
    /// accepts; an unknown registry is still refused.
    #[test]
    fn association_accepts_nicbr_and_rejects_unknown_rir() {
        let svc = Ec2Service::new();
        seed_ipam(&svc);
        let created = body(
            create_ipam_internet_registry_association(
                &svc,
                &req(
                    "CreateIpamInternetRegistryAssociation",
                    &[
                        ("IpamId", "ipam-1"),
                        ("Rir", "nicbr"),
                        ("OrganizationHandle", "ORG-BR"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(created.contains("<rir>nicbr</rir>"), "{created}");
        let err = err_of(create_ipam_internet_registry_association(
            &svc,
            &req(
                "CreateIpamInternetRegistryAssociation",
                &[
                    ("IpamId", "ipam-1"),
                    ("Rir", "afrinic-x"),
                    ("OrganizationHandle", "ORG-X"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    /// Create an association, without enabling it: it cannot publish yet.
    fn make_pending_association(svc: &Ec2Service) -> String {
        seed_ipam(svc);
        let b = body(
            create_ipam_internet_registry_association(
                svc,
                &req(
                    "CreateIpamInternetRegistryAssociation",
                    &[
                        ("IpamId", "ipam-1"),
                        ("Rir", "arin"),
                        ("OrganizationHandle", "ORG-1"),
                    ],
                ),
            )
            .unwrap(),
        );
        b.split("<ipamInternetRegistryAssociationId>")
            .nth(1)
            .unwrap()
            .split("</ipamInternetRegistryAssociationId>")
            .next()
            .unwrap()
            .to_string()
    }

    fn enable(svc: &Ec2Service, id: &str, service_uri: &str, child_handle: &str) -> String {
        body(
            enable_ipam_internet_registry_association(
                svc,
                &req(
                    "EnableIpamInternetRegistryAssociation",
                    &[
                        ("IpamInternetRegistryAssociationId", id),
                        ("RpkiVersion", "1"),
                        ("ServiceUri", service_uri),
                        ("ChildHandle", child_handle),
                        ("ParentHandle", "parent"),
                        ("ParentBpkiTa", "TA=="),
                    ],
                ),
            )
            .unwrap(),
        )
    }

    /// An association that has been enabled against the registry, which is
    /// what a registration needs.
    fn make_association(svc: &Ec2Service) -> String {
        let id = make_pending_association(svc);
        enable(svc, &id, "https://rpki.example/up-down", "child");
        id
    }

    fn register(svc: &Ec2Service, id: &str, cidr: &str, max_length: Option<&str>) {
        let mut params: Vec<(&str, &str)> = vec![
            ("IpamInternetRegistryAssociationId", id),
            ("Cidr", cidr),
            ("Asn.1", "64512"),
        ];
        if let Some(m) = max_length {
            params.push(("MaxLength", m));
        }
        create_ipam_routing_policy_registration(
            svc,
            &req("CreateIpamRoutingPolicyRegistration", &params),
        )
        .unwrap();
    }

    fn registrations(svc: &Ec2Service, id: &str, params: &[(&str, &str)]) -> String {
        let mut all: Vec<(&str, &str)> = vec![("IpamInternetRegistryAssociationId", id)];
        all.extend_from_slice(params);
        body(
            get_ipam_routing_policy_registrations(
                svc,
                &req("GetIpamRoutingPolicyRegistrations", &all),
            )
            .unwrap(),
        )
    }

    fn stored_child_request(svc: &Ec2Service, id: &str) -> String {
        svc.state
            .read()
            .regional("000000000000", "us-east-1")
            .unwrap()
            .ipam_ir_associations
            .get(id)
            .unwrap()
            .child_request_xml
            .clone()
            .unwrap()
    }

    fn batch(svc: &Ec2Service, id: &str, delta_json: &str) -> Result<AwsResponse, AwsServiceError> {
        batch_modify_ipam_routing_policy_registrations(
            svc,
            &req(
                "BatchModifyIpamRoutingPolicyRegistrations",
                &[
                    ("IpamInternetRegistryAssociationId", id),
                    ("DeltaJson", delta_json),
                ],
            ),
        )
    }

    fn deltas(svc: &Ec2Service, id: &str, params: &[(&str, &str)]) -> String {
        let mut all: Vec<(&str, &str)> = vec![("IpamInternetRegistryAssociationId", id)];
        all.extend_from_slice(params);
        body(
            get_ipam_routing_policy_registration_deltas(
                svc,
                &req("GetIpamRoutingPolicyRegistrationDeltas", &all),
            )
            .unwrap(),
        )
    }

    fn elements(body: &str, tag: &str) -> Vec<String> {
        body.split(&format!("<{tag}>"))
            .skip(1)
            .map(|s| {
                s.split(&format!("</{tag}>"))
                    .next()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    }

    fn with_association<T>(
        svc: &Ec2Service,
        id: &str,
        f: impl FnOnce(&mut IpamInternetRegistryAssociation) -> T,
    ) -> T {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut("000000000000", "us-east-1");
        f(state.ipam_ir_associations.get_mut(id).unwrap())
    }

    /// Push every recorded idempotency token out of the window, the way the
    /// clock does to a record nobody retried in time.
    fn age_client_tokens(svc: &Ec2Service, id: &str) {
        let stale = (Utc::now() - chrono::Duration::seconds(CLIENT_TOKEN_TTL_SECONDS + 60))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        with_association(svc, id, |a| {
            for record in a.client_tokens.values_mut() {
                record.recorded_at = stale.clone();
            }
        });
    }

    /// A finding's `roaSet` carries `IpamRouteOriginAuthorization`, whose
    /// prefix member is `prefix`; `cidr` belongs to a different shape and an
    /// SDK discards it.
    #[test]
    fn route_protection_findings_use_the_modeled_roa_members() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", Some("24"));

        let b = body(
            get_ipam_route_protection_findings(
                &svc,
                &req("GetIpamRouteProtectionFindings", &[("IpamId", "ipam-1")]),
            )
            .unwrap(),
        );
        assert!(b.contains("<prefix>192.0.2.0/24</prefix>"), "{b}");
        assert!(
            !b.contains("<roaSet><item><cidr>"),
            "cidr is the wrong member name here: {b}"
        );
        // `IpamRpkiStrength` is `strict | permissive` -- nothing else.
        assert!(b.contains("<rpkiStrength>strict</rpkiStrength>"), "{b}");
        assert!(!b.contains("strong"), "{b}");
    }

    /// The child request is a document the caller hands to the RIR, so every
    /// value interpolated into it has to be entity-escaped. The response
    /// escapes the blob as a whole, so an unescaped `&` or `"` would look fine
    /// on the wire and only break when the registry parses the document.
    #[test]
    fn the_child_request_escapes_every_interpolated_value() {
        let svc = Ec2Service::new();
        let id = make_pending_association(&svc);
        enable(
            &svc,
            &id,
            "https://rpki.example/up-down?src=a&v=2",
            "ch\"ild",
        );

        let doc = stored_child_request(&svc, &id);
        assert!(
            doc.contains("service_uri=\"https://rpki.example/up-down?src=a&amp;v=2\""),
            "{doc}"
        );
        assert!(doc.contains("child_handle=\"ch&quot;ild\""), "{doc}");
        // The attribute never closes early, so the document stays parseable.
        assert_eq!(doc.matches('"').count(), 8, "{doc}");
    }

    /// A DryRun validates the request, so it cannot report success for an
    /// association that does not exist.
    #[test]
    fn a_dry_run_still_resolves_the_association() {
        let svc = Ec2Service::new();
        let missing = "ipam-ir-assoc-nope";
        for r in [
            delete_ipam_internet_registry_association(
                &svc,
                &req(
                    "DeleteIpamInternetRegistryAssociation",
                    &[
                        ("IpamInternetRegistryAssociationId", missing),
                        ("DryRun", "true"),
                    ],
                ),
            ),
            delete_ipam_routing_policy_registration(
                &svc,
                &req(
                    "DeleteIpamRoutingPolicyRegistration",
                    &[
                        ("IpamInternetRegistryAssociationId", missing),
                        ("Cidr", "192.0.2.0/24"),
                        ("DryRun", "true"),
                    ],
                ),
            ),
        ] {
            assert_eq!(
                err_of(r).code(),
                "InvalidIpamInternetRegistryAssociationId.NotFound"
            );
        }

        // And a dry run against a live association changes nothing.
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);
        delete_ipam_routing_policy_registration(
            &svc,
            &req(
                "DeleteIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "192.0.2.0/24"),
                    ("DryRun", "true"),
                ],
            ),
        )
        .unwrap();
        assert!(registrations(&svc, &id, &[]).contains("192.0.2.0/24"));
    }

    /// A dry run reaches the same verdict as the real call, so it runs after
    /// every existence check rather than before them: otherwise it reports
    /// success and the call it was rehearsing fails.
    #[test]
    fn a_dry_run_reaches_the_same_verdict_as_the_real_call() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);

        // Creating an already-registered CIDR is a conflict, dry run or not.
        let err = err_of(create_ipam_routing_policy_registration(
            &svc,
            &req(
                "CreateIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "192.0.2.0/24"),
                    ("Asn.1", "64512"),
                    ("DryRun", "true"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");

        // And modifying one that was never registered is still a not-found.
        let err = err_of(modify_ipam_routing_policy_registration(
            &svc,
            &req(
                "ModifyIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "198.51.100.0/24"),
                    ("Asn.1", "64512"),
                    ("DryRun", "true"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidIpamRoutingPolicyRegistration.NotFound");

        // A dry-run create naming an IPAM that does not exist is the very
        // failure a dry run exists to surface.
        let err = err_of(create_ipam_internet_registry_association(
            &svc,
            &req(
                "CreateIpamInternetRegistryAssociation",
                &[
                    ("IpamId", "ipam-ghost"),
                    ("Rir", "arin"),
                    ("OrganizationHandle", "ORG-1"),
                    ("DryRun", "true"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidIpamId.NotFound");
    }

    /// The model requires the registrations to be removed before the
    /// association goes, so a delete that would orphan published ROAs is
    /// refused -- on a dry run exactly as for real.
    #[test]
    fn deleting_an_association_requires_its_registrations_to_be_gone() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);

        for dry in ["false", "true"] {
            let err = err_of(delete_ipam_internet_registry_association(
                &svc,
                &req(
                    "DeleteIpamInternetRegistryAssociation",
                    &[("IpamInternetRegistryAssociationId", &id), ("DryRun", dry)],
                ),
            ));
            assert_eq!(err.code(), "DependencyViolation", "DryRun={dry}");
        }
        // The association and its registration are both still there.
        assert!(registrations(&svc, &id, &[]).contains("192.0.2.0/24"));

        delete_ipam_routing_policy_registration(
            &svc,
            &req(
                "DeleteIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "192.0.2.0/24"),
                ],
            ),
        )
        .unwrap();
        let b = body(
            delete_ipam_internet_registry_association(
                &svc,
                &req(
                    "DeleteIpamInternetRegistryAssociation",
                    &[("IpamInternetRegistryAssociationId", &id)],
                ),
            )
            .unwrap(),
        );
        assert!(b.contains("<state>delete-complete</state>"), "{b}");
    }

    /// `MaxLength` carries `@range 0..48` and must cover at least the prefix.
    #[test]
    fn max_length_is_bounded_by_the_model_and_the_prefix() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        for bad in ["49", "200", "16"] {
            let err = err_of(create_ipam_routing_policy_registration(
                &svc,
                &req(
                    "CreateIpamRoutingPolicyRegistration",
                    &[
                        ("IpamInternetRegistryAssociationId", &id),
                        ("Cidr", "192.0.2.0/24"),
                        ("Asn.1", "64512"),
                        ("MaxLength", bad),
                    ],
                ),
            ));
            assert_eq!(err.code(), "InvalidParameterValue", "MaxLength={bad}");
        }
        register(&svc, &id, "192.0.2.0/24", Some("32"));
    }

    /// Modify takes only `Asns` as required, so the members it leaves out keep
    /// the values the registration already carries instead of being cleared.
    #[test]
    fn modify_is_a_partial_update() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        create_ipam_routing_policy_registration(
            &svc,
            &req(
                "CreateIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "10.0.0.0/16"),
                    ("Asn.1", "64512"),
                    ("MaxLength", "24"),
                    ("Description", "prod prefix"),
                    ("PermitMoreSpecificAnnouncements", "true"),
                ],
            ),
        )
        .unwrap();

        modify_ipam_routing_policy_registration(
            &svc,
            &req(
                "ModifyIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "10.0.0.0/16"),
                    ("Asn.1", "64513"),
                ],
            ),
        )
        .unwrap();

        let b = registrations(&svc, &id, &[]);
        assert!(b.contains("<item>64513</item>"), "{b}");
        assert!(b.contains("<maxLength>24</maxLength>"), "{b}");
        assert!(b.contains("<description>prod prefix</description>"), "{b}");
        assert!(
            b.contains("<permitMoreSpecificAnnouncements>true</permitMoreSpecificAnnouncements>"),
            "{b}"
        );
        assert!(b.contains("<state>update-complete</state>"), "{b}");

        // The ROAs the registration publishes keep the max length too.
        let b = body(
            get_ipam_route_origin_authorizations(
                &svc,
                &req(
                    "GetIpamRouteOriginAuthorizations",
                    &[("IpamInternetRegistryAssociationId", &id)],
                ),
            )
            .unwrap(),
        );
        assert!(b.contains("<maxLength>24</maxLength>"), "{b}");
    }

    /// A registration publishes through the association's RPKI service, which
    /// only exists once the association has been enabled.
    #[test]
    fn a_registration_needs_an_enabled_association() {
        let svc = Ec2Service::new();
        let id = make_pending_association(&svc);

        let err = err_of(create_ipam_routing_policy_registration(
            &svc,
            &req(
                "CreateIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "192.0.2.0/24"),
                    ("Asn.1", "64512"),
                ],
            ),
        ));
        assert_eq!(err.code(), "IncorrectState");

        let err = err_of(batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"192.0.2.0/24","asns":["64512"]}]}"#,
        ));
        assert_eq!(err.code(), "IncorrectState");

        // Enabling it opens the association up.
        enable(&svc, &id, "https://rpki.example/up-down", "child");
        register(&svc, &id, "192.0.2.0/24", None);
        assert!(registrations(&svc, &id, &[]).contains("192.0.2.0/24"));
    }

    /// An ASN is a number, and a hand-written delta document spells it as one.
    /// Dropping those ASNs would publish a registration authorizing nobody.
    #[test]
    fn a_batch_accepts_asns_written_as_numbers() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"192.0.2.0/24","asns":[64512,"64513"]}]}"#,
        )
        .unwrap();

        let b = registrations(&svc, &id, &[]);
        assert!(b.contains("<item>64512</item>"), "{b}");
        assert!(b.contains("<item>64513</item>"), "{b}");

        // And the registration publishes the route origin authorizations that
        // make the finding `valid` rather than `unknown`.
        let b = body(
            get_ipam_route_protection_findings(
                &svc,
                &req("GetIpamRouteProtectionFindings", &[("IpamId", "ipam-1")]),
            )
            .unwrap(),
        );
        assert!(b.contains("<rpkiStatus>valid</rpkiStatus>"), "{b}");
    }

    /// The delta records the whole document as published, so an entry the
    /// caller mistyped fails the request instead of leaving an audit trail
    /// claiming registrations that were never applied.
    #[test]
    fn a_batch_entry_that_cannot_be_applied_fails_the_whole_request() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        for bad in [
            r#"{"add":[{"cidr":"192.0.2.0/24","asns":["64512"]},{"asns":["64513"]}]}"#,
            r#"{"add":[{"cidr":"192.0.2.0/24","asns":["64512"]},{"cidr":198,"asns":["64513"]}]}"#,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":["64512"],"maxLength":200}]}"#,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":["64512"],"maxLength":8}]}"#,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":[]}]}"#,
            r#"{"add":[{"cidr":"10.0.0.0/16"}]}"#,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":["64512"],"maxLength":"24"}]}"#,
            r#"{"remove":[{"asn":"64512"}]}"#,
        ] {
            let err = err_of(batch(&svc, &id, bad));
            assert_eq!(err.code(), "InvalidParameterValue", "{bad}");
        }
        // Removing a CIDR that was never registered is a not-found, not a
        // delta claiming a removal that did not happen.
        let err = err_of(batch(&svc, &id, r#"{"remove":["203.0.113.0/24"]}"#));
        assert_eq!(err.code(), "InvalidIpamRoutingPolicyRegistration.NotFound");

        // Nothing was applied and no delta was recorded.
        let b = registrations(&svc, &id, &[]);
        assert!(!b.contains("192.0.2.0/24"), "{b}");
        let b = body(
            get_ipam_routing_policy_registration_deltas(
                &svc,
                &req(
                    "GetIpamRoutingPolicyRegistrationDeltas",
                    &[("IpamInternetRegistryAssociationId", &id)],
                ),
            )
            .unwrap(),
        );
        assert!(!b.contains("<deltaId>"), "{b}");
    }

    /// A batch `add` is documented to create or update, so a second document
    /// for the same CIDR updates it -- partially, the way Modify does.
    #[test]
    fn a_batch_add_updates_a_cidr_it_already_registered() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":["64512"],"maxLength":24}]}"#,
        )
        .unwrap();
        batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":[64513]}]}"#,
        )
        .unwrap();

        let b = registrations(&svc, &id, &[]);
        assert_eq!(b.matches("<cidr>10.0.0.0/16</cidr>").count(), 1, "{b}");
        assert!(b.contains("<item>64513</item>"), "{b}");
        assert!(b.contains("<maxLength>24</maxLength>"), "{b}");
        assert!(b.contains("<state>update-complete</state>"), "{b}");
    }

    /// A retry under the original `ClientToken` replays the first call's
    /// result instead of failing on the change that call already made.
    #[test]
    fn a_client_token_replays_the_original_result() {
        let svc = Ec2Service::new();
        seed_ipam(&svc);
        let create = |token: &str| {
            body(
                create_ipam_internet_registry_association(
                    &svc,
                    &req(
                        "CreateIpamInternetRegistryAssociation",
                        &[
                            ("IpamId", "ipam-1"),
                            ("Rir", "arin"),
                            ("OrganizationHandle", "ORG-1"),
                            ("ClientToken", token),
                        ],
                    ),
                )
                .unwrap(),
            )
        };
        let first = create("token-a");
        assert_eq!(first, create("token-a"), "the retry replays the original");
        assert_ne!(first, create("token-b"), "a new token is a new association");

        let id = make_association(&svc);
        let register_once = |token: &str| {
            body(
                create_ipam_routing_policy_registration(
                    &svc,
                    &req(
                        "CreateIpamRoutingPolicyRegistration",
                        &[
                            ("IpamInternetRegistryAssociationId", &id),
                            ("Cidr", "192.0.2.0/24"),
                            ("Asn.1", "64512"),
                            ("ClientToken", token),
                        ],
                    ),
                )
                .unwrap(),
            )
        };
        let first = register_once("token-c");
        assert_eq!(first, register_once("token-c"));
        // The replay minted no second delta and no second registration.
        let b = registrations(&svc, &id, &[]);
        assert_eq!(b.matches("<cidr>192.0.2.0/24</cidr>").count(), 1, "{b}");
        let b = body(
            get_ipam_routing_policy_registration_deltas(
                &svc,
                &req(
                    "GetIpamRoutingPolicyRegistrationDeltas",
                    &[("IpamInternetRegistryAssociationId", &id)],
                ),
            )
            .unwrap(),
        );
        assert_eq!(b.matches("<deltaId>").count(), 1, "{b}");
    }

    /// A time bound is compared as an instant, so a delta recorded in the same
    /// second as the bound is not silently dropped, and a malformed bound is
    /// rejected rather than filtering everything out.
    #[test]
    fn delta_time_bounds_compare_instants() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);

        // A whole-second bound at the epoch start still includes the delta.
        let b = body(
            get_ipam_routing_policy_registration_deltas(
                &svc,
                &req(
                    "GetIpamRoutingPolicyRegistrationDeltas",
                    &[
                        ("IpamInternetRegistryAssociationId", &id),
                        ("StartTime", "2000-01-01T00:00:00Z"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(b.contains("<deltaId>"), "{b}");

        let err = err_of(get_ipam_routing_policy_registration_deltas(
            &svc,
            &req(
                "GetIpamRoutingPolicyRegistrationDeltas",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("StartTime", "banana"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    /// `MaxResults` bounds a page and the `nextToken` it returns fetches the
    /// rest, rather than every read handing back the whole set.
    #[test]
    fn reads_page_and_round_trip_the_next_token() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        for i in 0..7 {
            register(&svc, &id, &format!("10.{i}.0.0/16"), None);
        }

        let first = registrations(&svc, &id, &[("MaxResults", "5")]);
        assert_eq!(first.matches("<cidr>").count(), 5, "{first}");
        let token = first
            .split("<nextToken>")
            .nth(1)
            .unwrap_or_else(|| panic!("no nextToken in {first}"))
            .split("</nextToken>")
            .next()
            .unwrap()
            .to_string();

        let second = registrations(&svc, &id, &[("MaxResults", "5"), ("NextToken", &token)]);
        assert_eq!(second.matches("<cidr>").count(), 2, "{second}");
        assert!(!second.contains("<nextToken>"), "{second}");

        // The deltas each registration produced page the same way.
        let b = body(
            get_ipam_routing_policy_registration_deltas(
                &svc,
                &req(
                    "GetIpamRoutingPolicyRegistrationDeltas",
                    &[
                        ("IpamInternetRegistryAssociationId", &id),
                        ("MaxResults", "5"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert_eq!(b.matches("<deltaId>").count(), 5, "{b}");
        assert!(b.contains("<nextToken>"), "{b}");

        // `IpamMaxResults` is `@range 5..1000`.
        let err = err_of(get_ipam_routing_policy_registrations(
            &svc,
            &req(
                "GetIpamRoutingPolicyRegistrations",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("MaxResults", "1"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    /// The `Filters` these operations model narrow the result rather than
    /// being accepted and discarded.
    #[test]
    fn filters_narrow_the_results() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);

        let describe = |params: &[(&str, &str)]| {
            body(
                describe_ipam_internet_registry_associations(
                    &svc,
                    &req("DescribeIpamInternetRegistryAssociations", params),
                )
                .unwrap(),
            )
        };
        assert!(describe(&[("Filter.1.Name", "rir"), ("Filter.1.Value.1", "arin")]).contains(&id));
        assert!(!describe(&[("Filter.1.Name", "rir"), ("Filter.1.Value.1", "ripe")]).contains(&id));
        assert!(
            !describe(&[("Filter.1.Name", "nonsense"), ("Filter.1.Value.1", "arin")]).contains(&id),
            "an unknown filter name matches nothing"
        );

        // The per-CIDR and per-ASN views filter too.
        let cidrs = body(
            get_ipam_internet_registry_association_cidrs(
                &svc,
                &req(
                    "GetIpamInternetRegistryAssociationCidrs",
                    &[
                        ("IpamInternetRegistryAssociationId", &id),
                        ("Filter.1.Name", "cidr"),
                        ("Filter.1.Value.1", "198.51.100.0/24"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(!cidrs.contains("192.0.2.0/24"), "{cidrs}");
        let asns = body(
            get_ipam_internet_registry_association_asns(
                &svc,
                &req(
                    "GetIpamInternetRegistryAssociationAsns",
                    &[
                        ("IpamInternetRegistryAssociationId", &id),
                        ("Filter.1.Name", "asn"),
                        ("Filter.1.Value.1", "64512"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(asns.contains("<asn>64512</asn>"), "{asns}");
    }

    /// A token reused with different parameters is not a retry. Replaying the
    /// original result there reports success for a change nobody made: the
    /// worst case is a delete of a CIDR that is still registered, which the
    /// caller then meets again as a `DependencyViolation` on the association
    /// it was told was empty.
    #[test]
    fn a_client_token_reused_with_different_parameters_is_rejected() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);
        register(&svc, &id, "198.51.100.0/24", None);

        let delete = |cidr: &str| {
            delete_ipam_routing_policy_registration(
                &svc,
                &req(
                    "DeleteIpamRoutingPolicyRegistration",
                    &[
                        ("IpamInternetRegistryAssociationId", &id),
                        ("Cidr", cidr),
                        ("ClientToken", "token-delete"),
                    ],
                ),
            )
        };
        let first = body(delete("192.0.2.0/24").unwrap());
        let err = err_of(delete("198.51.100.0/24"));
        assert_eq!(err.code(), "IdempotentParameterMismatch");
        let b = registrations(&svc, &id, &[]);
        assert!(b.contains("198.51.100.0/24"), "nothing was removed: {b}");
        // The same call under the same token still replays.
        assert_eq!(first, body(delete("192.0.2.0/24").unwrap()));

        // A create that reuses a token for a different registry handle is not
        // a retry either.
        let create = |handle: &str| {
            create_ipam_internet_registry_association(
                &svc,
                &req(
                    "CreateIpamInternetRegistryAssociation",
                    &[
                        ("IpamId", "ipam-1"),
                        ("Rir", "arin"),
                        ("OrganizationHandle", handle),
                        ("ClientToken", "token-create"),
                    ],
                ),
            )
        };
        create("ORG-1").unwrap();
        assert_eq!(
            err_of(create("ORG-2")).code(),
            "IdempotentParameterMismatch"
        );

        // And so is a batch document that changed under a reused token.
        let batch_once = |doc: &str| {
            batch_modify_ipam_routing_policy_registrations(
                &svc,
                &req(
                    "BatchModifyIpamRoutingPolicyRegistrations",
                    &[
                        ("IpamInternetRegistryAssociationId", &id),
                        ("DeltaJson", doc),
                        ("ClientToken", "token-batch"),
                    ],
                ),
            )
        };
        batch_once(r#"{"add":[{"cidr":"203.0.113.0/24","asns":["64512"]}]}"#).unwrap();
        let err = err_of(batch_once(r#"{"remove":["198.51.100.0/24"]}"#));
        assert_eq!(err.code(), "IdempotentParameterMismatch");
        let b = registrations(&svc, &id, &[]);
        assert!(b.contains("198.51.100.0/24"), "{b}");
    }

    /// `ClientToken` is an `@idempotencyToken`, so an SDK fills a fresh one in
    /// on every call rather than only on retries: the records have to age out
    /// and stay capped, or a create/delete loop would leave one dead record
    /// per call in the association and in every snapshot of it, forever.
    #[test]
    fn client_token_records_age_out_and_stay_bounded() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);

        let modify = |token: &str| {
            body(
                modify_ipam_routing_policy_registration(
                    &svc,
                    &req(
                        "ModifyIpamRoutingPolicyRegistration",
                        &[
                            ("IpamInternetRegistryAssociationId", &id),
                            ("Cidr", "192.0.2.0/24"),
                            ("Asn.1", "64512"),
                            ("ClientToken", token),
                        ],
                    ),
                )
                .unwrap(),
            )
        };
        let first = modify("token-m");
        assert_eq!(first, modify("token-m"), "a retry in the window replays");

        age_client_tokens(&svc, &id);
        assert_ne!(
            first,
            modify("token-m"),
            "an aged-out token no longer replays"
        );

        // Every call mints its own token, so the cap is what bounds the map.
        for i in 0..CLIENT_TOKEN_MAX_RECORDS + 50 {
            modify(&format!("token-{i}"));
        }
        let kept = with_association(&svc, &id, |a| a.client_tokens.len());
        assert!(kept <= CLIENT_TOKEN_MAX_RECORDS, "{kept} records kept");
    }

    /// A batch document is checked against the state it would itself leave, so
    /// it can remove a CIDR it adds; the removal of a CIDR the document
    /// neither holds nor adds is still a not-found.
    #[test]
    fn a_batch_can_remove_a_cidr_it_adds() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);

        batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":["64512"]}],"remove":["10.0.0.0/16"]}"#,
        )
        .unwrap();
        let b = registrations(&svc, &id, &[]);
        assert!(!b.contains("10.0.0.0/16"), "{b}");
        // The document was published, so it left the audit trail behind.
        assert_eq!(elements(&deltas(&svc, &id, &[]), "deltaId").len(), 1);

        let err = err_of(batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"10.0.0.0/16","asns":["64512"]}],"remove":["203.0.113.0/24"]}"#,
        ));
        assert_eq!(err.code(), "InvalidIpamRoutingPolicyRegistration.NotFound");
    }

    /// Modify is a partial update, so an omitted member keeps its value --
    /// which leaves the caller needing a spelling that says "remove this". An
    /// empty value clears the member on the wire, and `null` clears it in a
    /// delta document.
    #[test]
    fn optional_members_can_be_cleared() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        create_ipam_routing_policy_registration(
            &svc,
            &req(
                "CreateIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "10.0.0.0/16"),
                    ("Asn.1", "64512"),
                    ("MaxLength", "24"),
                    ("Description", "prod prefix"),
                    ("PermitMoreSpecificAnnouncements", "true"),
                ],
            ),
        )
        .unwrap();

        modify_ipam_routing_policy_registration(
            &svc,
            &req(
                "ModifyIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "10.0.0.0/16"),
                    ("Asn.1", "64512"),
                    ("MaxLength", ""),
                    ("Description", ""),
                    ("PermitMoreSpecificAnnouncements", ""),
                ],
            ),
        )
        .unwrap();

        let b = registrations(&svc, &id, &[]);
        assert!(!b.contains("<maxLength>"), "{b}");
        assert!(!b.contains("<description>"), "{b}");
        assert!(!b.contains("<permitMoreSpecificAnnouncements>"), "{b}");
        // The delta says what was asked for: a cleared member is `null`, which
        // is how a batch document spells the same change.
        let recorded = with_association(&svc, &id, |a| {
            a.deltas
                .last()
                .expect("a delta was recorded")
                .delta_json
                .clone()
        });
        assert!(recorded.contains(r#""maxLength":null"#), "{recorded}");
        assert!(recorded.contains(r#""description":null"#), "{recorded}");

        // A batch entry clears with `null` and leaves an omitted member alone.
        batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"192.0.2.0/24","asns":["64512"],"maxLength":25,
                        "description":"edge prefix"}]}"#,
        )
        .unwrap();
        batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"192.0.2.0/24","asns":["64512"],"description":null}]}"#,
        )
        .unwrap();
        let b = registrations(&svc, &id, &[("Cidr", "192.0.2.0/24")]);
        assert!(!b.contains("<description>"), "{b}");
        assert!(b.contains("<maxLength>25</maxLength>"), "{b}");
    }

    /// Deltas are appended, so an offset into the reversed list moves under a
    /// caller who pages: `reverse` pages by delta id instead, and the second
    /// page cannot repeat what the first already reported.
    #[test]
    fn reverse_delta_pages_do_not_repeat_when_deltas_are_appended() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        for i in 0..7 {
            register(&svc, &id, &format!("10.{i}.0.0/16"), None);
        }

        let reverse = [("ChronologicalOrder", "reverse"), ("MaxResults", "5")];
        let first = deltas(&svc, &id, &reverse);
        let seen = elements(&first, "deltaId");
        assert_eq!(seen.len(), 5, "{first}");
        let token = elements(&first, "nextToken")
            .pop()
            .unwrap_or_else(|| panic!("no nextToken in {first}"));

        // Two more deltas land between the two pages.
        register(&svc, &id, "10.7.0.0/16", None);
        register(&svc, &id, "10.8.0.0/16", None);

        let mut params = reverse.to_vec();
        params.push(("NextToken", &token));
        let second = deltas(&svc, &id, &params);
        let rest = elements(&second, "deltaId");
        assert_eq!(rest.len(), 2, "{second}");
        assert!(
            rest.iter().all(|d| !seen.contains(d)),
            "a second page must not repeat the first: {first} {second}"
        );
        assert!(!second.contains("<nextToken>"), "{second}");

        // A `reverse` token that names no delta is rejected rather than
        // silently restarting the caller from the newest one.
        let bad: Vec<(&str, &str)> = vec![
            ("IpamInternetRegistryAssociationId", &id),
            ("ChronologicalOrder", "reverse"),
            ("NextToken", "ipam-delta-nope"),
        ];
        let err = err_of(get_ipam_routing_policy_registration_deltas(
            &svc,
            &req("GetIpamRoutingPolicyRegistrationDeltas", &bad),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    /// A `MaxResults` that is not a number is rejected the way a `NextToken`
    /// that is not a cursor is: taking it as "no limit" would hand back the
    /// whole set unpaginated.
    #[test]
    fn a_max_results_that_is_not_a_number_is_rejected() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        for i in 0..7 {
            register(&svc, &id, &format!("10.{i}.0.0/16"), None);
        }

        let err = err_of(get_ipam_routing_policy_registrations(
            &svc,
            &req(
                "GetIpamRoutingPolicyRegistrations",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("MaxResults", "abc"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");

        let err = err_of(get_ipam_routing_policy_registration_deltas(
            &svc,
            &req(
                "GetIpamRoutingPolicyRegistrationDeltas",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("MaxResults", "abc"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");

        let err = err_of(get_ipam_routing_policy_registrations(
            &svc,
            &req(
                "GetIpamRoutingPolicyRegistrations",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("NextToken", "abc"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    /// Publishing needs the association's RPKI service; un-publishing does
    /// not. An association restored from a snapshot taken before registrations
    /// were gated still carries them in `pending-enable`, and the association
    /// cannot be deleted while they remain -- gating removals too would strand
    /// it and its ROAs for good.
    #[test]
    fn removing_a_registration_does_not_need_an_enabled_association() {
        let svc = Ec2Service::new();
        let id = make_association(&svc);
        register(&svc, &id, "192.0.2.0/24", None);
        register(&svc, &id, "198.51.100.0/24", None);
        with_association(&svc, &id, |a| a.state = "pending-enable".to_string());

        let err = err_of(create_ipam_routing_policy_registration(
            &svc,
            &req(
                "CreateIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "203.0.113.0/24"),
                    ("Asn.1", "64512"),
                ],
            ),
        ));
        assert_eq!(err.code(), "IncorrectState");
        let err = err_of(batch(
            &svc,
            &id,
            r#"{"add":[{"cidr":"203.0.113.0/24","asns":["64512"]}],"remove":["192.0.2.0/24"]}"#,
        ));
        assert_eq!(err.code(), "IncorrectState");

        // Both spellings of a removal go through, so the association can be
        // emptied and then deleted.
        delete_ipam_routing_policy_registration(
            &svc,
            &req(
                "DeleteIpamRoutingPolicyRegistration",
                &[
                    ("IpamInternetRegistryAssociationId", &id),
                    ("Cidr", "192.0.2.0/24"),
                ],
            ),
        )
        .unwrap();
        batch(&svc, &id, r#"{"remove":["198.51.100.0/24"]}"#).unwrap();
        let b = body(
            delete_ipam_internet_registry_association(
                &svc,
                &req(
                    "DeleteIpamInternetRegistryAssociation",
                    &[("IpamInternetRegistryAssociationId", &id)],
                ),
            )
            .unwrap(),
        );
        assert!(b.contains("<state>delete-complete</state>"), "{b}");
    }
}
