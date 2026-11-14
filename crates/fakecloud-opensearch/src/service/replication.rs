//! Cross-Region replication between two domains of one account
//! (`AddReplicationPeer`, `DescribeDomainReplication`,
//! `RemoveReplicationPeer`, `SwitchoverReplication`, `FailoverReplication`).
//!
//! A relationship is recorded on both domains as a [`ReplicationLink`] naming
//! the other, with this domain's role and the relationship's status. Status
//! transitions that AWS runs asynchronously (`PENDING_SETUP`,
//! `SWITCHING_OVER`, `FAILING_OVER`, `PENDING_REMOVAL`) are reported by the
//! call that starts them and have settled by the next replication call that
//! reads the relationship, the way domain creation settles on the next
//! describe.

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{body, label, not_found_domain, not_found_generic, ok, req_str, validation, Labels};
use crate::state::{OpenSearchState, ReplicationLink};

pub(super) const PRIMARY: &str = "PRIMARY";
pub(super) const SECONDARY: &str = "SECONDARY";
const PENDING_SETUP: &str = "PENDING_SETUP";
const ACTIVE: &str = "ACTIVE";
const SWITCHING_OVER: &str = "SWITCHING_OVER";
const FAILING_OVER: &str = "FAILING_OVER";
const PENDING_REMOVAL: &str = "PENDING_REMOVAL";
const DETACHED: &str = "DETACHED";
const EJECTED: &str = "EJECTED";

/// Settle a transition in progress: setup and switchover end `ACTIVE`, a
/// failover ends with the former primary `EJECTED`, and a removal ends the
/// relationship (`None`).
fn settle(link: ReplicationLink) -> Option<ReplicationLink> {
    let status = match link.status.as_str() {
        PENDING_REMOVAL => return None,
        PENDING_SETUP | SWITCHING_OVER => ACTIVE,
        FAILING_OVER => EJECTED,
        other => other,
    }
    .to_string();
    Some(ReplicationLink { status, ..link })
}

fn settle_domain(st: &mut OpenSearchState, name: &str) {
    if let Some(d) = st.domains.get_mut(name) {
        d.replication = d.replication.take().and_then(settle);
    }
}

fn opposite(role: &str) -> &'static str {
    if role == PRIMARY {
        SECONDARY
    } else {
        PRIMARY
    }
}

/// The name of the live domain in `st` whose ARN is `arn`.
fn domain_name_by_arn(st: &OpenSearchState, arn: &str) -> Option<String> {
    st.domains
        .values()
        .find(|d| d.arn == arn && !d.deleted)
        .map(|d| d.name.clone())
}

/// The domain name an `es` domain ARN names, or `ValidationException`.
fn arn_domain_name<'a>(member: &str, arn: &'a str) -> Result<&'a str, AwsServiceError> {
    fakecloud_aws::arn::arn_resource(arn, "es")
        .and_then(|rest| rest.split_once(':'))
        .and_then(|(_, rest)| rest.split_once(':'))
        .and_then(|(_, resource)| resource.strip_prefix("domain/"))
        .filter(|n| !n.is_empty())
        .ok_or_else(|| validation(format!("{member} {arn} is not a valid domain ARN.")))
}

/// Resolve the request's `PeerDomainArn` against the caller's domain
/// `name`: both must exist in the caller's account. Returns the peer's
/// name.
fn resolve_pair(
    st: &OpenSearchState,
    name: &str,
    peer_arn: &str,
    account_id: &str,
) -> Result<String, AwsServiceError> {
    let peer_name = arn_domain_name("PeerDomainArn", peer_arn)?;
    if fakecloud_aws::arn::account_of(peer_arn) != Some(account_id) {
        return Err(validation(
            "The peer domain must be in the same AWS account as the domain.",
        ));
    }
    if !st.domains.contains_key(name) {
        return Err(not_found_domain(name));
    }
    domain_name_by_arn(st, peer_arn).ok_or_else(|| not_found_domain(peer_name))
}

/// The caller's domain's link, settled, which must name `peer_arn`.
fn link_with<'a>(
    st: &'a OpenSearchState,
    name: &str,
    peer_arn: &str,
) -> Result<&'a ReplicationLink, AwsServiceError> {
    st.domains
        .get(name)
        .and_then(|d| d.replication.as_ref())
        .filter(|l| l.peer_arn == peer_arn)
        .ok_or_else(|| validation(format!("Domain {name} does not replicate with {peer_arn}.")))
}

fn set_link(st: &mut OpenSearchState, name: &str, link: Option<ReplicationLink>) {
    if let Some(d) = st.domains.get_mut(name) {
        d.replication = link;
    }
}

fn status_response(arn: &str, status: Option<&str>) -> AwsResponse {
    let mut out = json!({ "DomainArn": arn });
    if let Some(s) = status {
        out["Status"] = Value::String(s.to_string());
    }
    ok(out)
}

impl super::OpenSearchService {
    pub(super) fn add_replication_peer(
        &self,
        l: &Labels,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = label(l.domain.as_deref())?;
        let b = body(req);
        let peer_arn = req_str(&b, "PeerDomainArn")?;
        let dry_run = b.get("DryRun").and_then(Value::as_bool).unwrap_or(false);
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let peer_name = resolve_pair(st, &name, &peer_arn, &req.account_id)?;
        let arn = st.domains[&name].arn.clone();
        if peer_name == name {
            return Err(validation(format!(
                "Domain {name} cannot replicate to itself."
            )));
        }
        if fakecloud_aws::arn::region_of(&arn) == fakecloud_aws::arn::region_of(&peer_arn) {
            return Err(validation(format!(
                "The peer domain must be in a different Region from domain {name}."
            )));
        }
        settle_domain(st, &name);
        settle_domain(st, &peer_name);
        // A relationship whose peer was deleted has ended; it gives way to a
        // new one.
        for n in [&name, &peer_name] {
            if let Some(link) = st.domains[n].replication.as_ref() {
                if link.status != DETACHED {
                    return Err(super::already_exists(format!(
                        "Domain {n} already replicates with {}.",
                        link.peer_arn
                    )));
                }
            }
        }
        if dry_run {
            return Ok(status_response(&arn, None));
        }
        let link = |peer: &str, role: &str| ReplicationLink {
            peer_arn: peer.to_string(),
            role: role.to_string(),
            status: PENDING_SETUP.to_string(),
            status_message: None,
        };
        set_link(st, &name, Some(link(&peer_arn, PRIMARY)));
        set_link(st, &peer_name, Some(link(&arn, SECONDARY)));
        Ok(status_response(&arn, Some(PENDING_SETUP)))
    }

    pub(super) fn describe_domain_replication(
        &self,
        l: &Labels,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = label(l.domain.as_deref())?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !st.domains.contains_key(&name) {
            return Err(not_found_domain(&name));
        }
        settle_domain(st, &name);
        let d = &st.domains[&name];
        let mut out = json!({ "DomainArn": d.arn, "Peers": [] });
        if let Some(link) = &d.replication {
            out["Role"] = json!(link.role);
            let mut peer = json!({
                "PeerDomainArn": link.peer_arn,
                "PeerRole": opposite(&link.role),
                "Status": link.status,
            });
            if let Some(m) = &link.status_message {
                peer["StatusMessage"] = json!(m);
            }
            out["Peers"] = json!([peer]);
        }
        Ok(ok(out))
    }

    pub(super) fn remove_replication_peer(
        &self,
        l: &Labels,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = label(l.domain.as_deref())?;
        let b = body(req);
        let peer_arn = req_str(&b, "PeerDomainArn")?;
        arn_domain_name("PeerDomainArn", &peer_arn)?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        if !st.domains.contains_key(&name) {
            return Err(not_found_domain(&name));
        }
        settle_domain(st, &name);
        let d = &st.domains[&name];
        let arn = d.arn.clone();
        let mut link = d
            .replication
            .clone()
            .filter(|l| l.peer_arn == peer_arn)
            .ok_or_else(|| {
                not_found_generic(&format!(
                    "Domain {name} has no replication relationship with {peer_arn}."
                ))
            })?;
        link.status = PENDING_REMOVAL.to_string();
        link.status_message = None;
        set_link(st, &name, Some(link));
        // The peer's side, when the peer still exists and still names us.
        if let Some(peer_name) = domain_name_by_arn(st, &peer_arn) {
            settle_domain(st, &peer_name);
            if let Some(peer_link) = st
                .domains
                .get_mut(&peer_name)
                .and_then(|p| p.replication.as_mut())
                .filter(|pl| pl.peer_arn == arn)
            {
                peer_link.status = PENDING_REMOVAL.to_string();
                peer_link.status_message = None;
            }
        }
        Ok(status_response(&arn, Some(PENDING_REMOVAL)))
    }

    pub(super) fn switchover_replication(
        &self,
        l: &Labels,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.promote(l, req, false)
    }

    pub(super) fn failover_replication(
        &self,
        l: &Labels,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let b = body(req);
        if b.get("AllowDataLoss").and_then(Value::as_bool) != Some(true) {
            return Err(validation(
                "AllowDataLoss must be true to acknowledge that writes which had not yet replicated are lost.",
            ));
        }
        self.promote(l, req, true)
    }

    /// Promote the secondary `DomainName` over its primary `PeerDomainArn`:
    /// a switchover keeps the relationship (roles swap), a failover leaves
    /// the former primary `EJECTED`.
    fn promote(
        &self,
        l: &Labels,
        req: &AwsRequest,
        failover: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = label(l.domain.as_deref())?;
        let b = body(req);
        let peer_arn = req_str(&b, "PeerDomainArn")?;
        let mut accounts = self.state.write();
        let st = accounts.get_or_create(&req.account_id);
        let peer_name = resolve_pair(st, &name, &peer_arn, &req.account_id)?;
        settle_domain(st, &name);
        settle_domain(st, &peer_name);
        let link = link_with(st, &name, &peer_arn)?;
        if link.role != SECONDARY {
            return Err(validation(format!(
                "Domain {name} is already the primary of its replication relationship."
            )));
        }
        if link.status != ACTIVE {
            return Err(validation(format!(
                "The replication relationship between {name} and {peer_arn} is {}; only an ACTIVE relationship can be promoted.",
                link.status
            )));
        }
        let arn = st.domains[&name].arn.clone();
        let (status, message) = if failover {
            (
                FAILING_OVER,
                Some(format!(
                    "A failover promoted {arn}. The former primary is read-only until the peer is removed."
                )),
            )
        } else {
            (SWITCHING_OVER, None)
        };
        let link = |peer: &str, role: &str| ReplicationLink {
            peer_arn: peer.to_string(),
            role: role.to_string(),
            status: status.to_string(),
            status_message: message.clone(),
        };
        set_link(st, &name, Some(link(&peer_arn, PRIMARY)));
        set_link(st, &peer_name, Some(link(&arn, SECONDARY)));
        Ok(status_response(&arn, Some(status)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(status: &str) -> ReplicationLink {
        ReplicationLink {
            peer_arn: "arn:aws:es:us-west-2:123456789012:domain/b".into(),
            role: PRIMARY.into(),
            status: status.into(),
            status_message: None,
        }
    }

    #[test]
    fn transitions_settle_to_their_end_state() {
        assert_eq!(settle(link(PENDING_SETUP)).unwrap().status, ACTIVE);
        assert_eq!(settle(link(SWITCHING_OVER)).unwrap().status, ACTIVE);
        assert_eq!(settle(link(FAILING_OVER)).unwrap().status, EJECTED);
        assert_eq!(settle(link(PENDING_REMOVAL)), None);
        for steady in [ACTIVE, EJECTED, DETACHED] {
            assert_eq!(settle(link(steady)).unwrap().status, steady);
        }
    }

    #[test]
    fn peer_arns_must_name_a_domain() {
        assert_eq!(
            arn_domain_name("P", "arn:aws:es:us-west-2:123456789012:domain/b").unwrap(),
            "b"
        );
        assert_eq!(
            arn_domain_name("P", "arn:aws-cn:es:cn-north-1:123456789012:domain/c").unwrap(),
            "c"
        );
        for bad in [
            "arn:aws:es:us-west-2:123456789012:application/b",
            "arn:aws:s3:::bucket",
            "domain/b",
            "arn:aws:es:us-west-2:123456789012:domain/",
        ] {
            assert!(arn_domain_name("P", bad).is_err(), "{bad}");
        }
    }
}
