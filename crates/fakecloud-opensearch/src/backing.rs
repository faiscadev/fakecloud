//! Keeps each domain's backing search cluster in line with its configuration.
//!
//! One routine, [`converge_domain`], serves every path that can change what a
//! domain's container should be -- `CreateDomain`, `UpdateDomainConfig`,
//! `UpgradeDomain`, CloudFormation, restart recovery -- so they cannot drift
//! apart. It reads the domain's current [`DomainSpec`], asks the runtime for a
//! container started from it, and settles the domain: `Processing` clears and
//! the endpoint becomes reachable, or the bring-up failure is recorded and the
//! domain degrades to control plane only. A spec that changed while the
//! container was booting is converged again before settling.

use std::sync::Arc;

use fakecloud_persistence::SnapshotHook;

use crate::runtime::{DomainSpec, OpenSearchRuntime};
use crate::state::{Domain, DomainDataPlane, SharedOpenSearchState};

/// `DomainProcessingStatus` while a new domain's cluster boots.
pub const CREATING: &str = "Creating";
/// `DomainProcessingStatus` while a configuration change replaces the cluster.
pub const MODIFYING: &str = "Modifying";
/// `DomainProcessingStatus` while an engine upgrade replaces the cluster.
pub const UPGRADING: &str = "UpgradingEngineVersion";

fn scope_tag() -> &'static str {
    fakecloud_core::data_volume::current_scope().tag()
}

/// The spec `domain`'s container should run, `None` when its engine version
/// has no image.
pub fn domain_spec(domain: &Domain) -> Option<DomainSpec> {
    DomainSpec::for_domain(domain, scope_tag())
}

/// Whether a runtime would back `domain` with a container, i.e. whether the
/// domain should report `Processing` until one answers.
pub fn backs(runtime_present: bool, domain: &Domain) -> bool {
    runtime_present && domain_spec(domain).is_some()
}

/// Mark `domain` as being brought up (or replaced) with `status`, dropping
/// its now-stale endpoint binding, when a runtime backs it. Returns whether a
/// [`converge_domain`] should follow.
pub fn begin(runtime_present: bool, domain: &mut Domain, status: &str) -> bool {
    if !backs(runtime_present, domain) {
        return false;
    }
    domain.processing = Some(status.to_string());
    domain.data_plane = None;
    domain.data_plane_error = None;
    true
}

/// Mark a configuration change that may replace `domain`'s container. Only a
/// change of the container spec does (a different image or node settings);
/// anything else leaves the running cluster alone. Returns whether a
/// [`converge_domain`] should follow.
pub fn begin_change(
    runtime: Option<&OpenSearchRuntime>,
    domain: &mut Domain,
    status: &str,
) -> bool {
    let Some(rt) = runtime else {
        return false;
    };
    let Some(spec) = domain_spec(domain) else {
        // Upgraded onto a version with no image: the old cluster no longer
        // matches the domain, so retire it.
        if let Some(cid) = rt.untrack(&domain.arn) {
            let rt = rt.clone();
            tokio::spawn(async move { rt.remove_container(&cid).await });
        }
        domain.data_plane = None;
        domain.processing = None;
        return false;
    };
    let unchanged = domain.data_plane.is_some() && rt.tracked_spec(&domain.arn) == Some(spec);
    if unchanged {
        return false;
    }
    if domain.processing.is_some() {
        // A bring-up is already in flight; it re-reads the spec before
        // settling, so it picks this change up.
        domain.processing = Some(status.to_string());
        return true;
    }
    begin(true, domain, status)
}

/// Bring `account`'s domain `name` up from its current spec and settle it.
/// `adopt` is a container a previous fakecloud process ran for it, reused if
/// it still matches. `persist` writes the snapshot once the domain settles.
pub async fn converge_domain(
    state: SharedOpenSearchState,
    runtime: Arc<OpenSearchRuntime>,
    account: String,
    name: String,
    mut adopt: Option<String>,
    persist: Option<SnapshotHook>,
) {
    loop {
        let target = {
            let accounts = state.read();
            accounts
                .get(&account)
                .and_then(|st| st.domains.get(&name))
                .filter(|d| !d.deleted)
                .map(|d| (d.arn.clone(), d.incarnation(), domain_spec(d)))
        };
        let Some((key, incarnation, spec)) = target else {
            return;
        };
        let Some(spec) = spec else {
            settle(&state, &account, &name, &incarnation, |d| {
                d.processing = None;
                d.data_plane = None;
            });
            break;
        };
        let result = runtime
            .ensure_domain(&key, &spec, adopt.take().as_deref())
            .await;
        // Settle under the lock, unless the domain moved on meanwhile.
        enum Outcome {
            Settled,
            Again,
            Gone,
        }
        let outcome = {
            let mut accounts = state.write();
            match accounts
                .get_mut(&account)
                .and_then(|st| st.domains.get_mut(&name))
                .filter(|d| !d.deleted && d.incarnation() == incarnation)
            {
                None => Outcome::Gone,
                Some(d) if domain_spec(d).as_ref() != Some(&spec) => Outcome::Again,
                Some(d) => {
                    d.processing = None;
                    match &result {
                        Ok(r) => {
                            d.data_plane = Some(DomainDataPlane {
                                container_id: r.container_id.clone(),
                                host: r.host.clone(),
                                port: r.port,
                            });
                            d.data_plane_error = None;
                        }
                        Err(e) => {
                            tracing::error!(domain = %key, error = %e, "search cluster failed to start; domain stays control-plane only");
                            d.data_plane = None;
                            d.data_plane_error = Some(e.to_string());
                        }
                    }
                    Outcome::Settled
                }
            }
        };
        match outcome {
            Outcome::Settled => break,
            Outcome::Again => continue,
            Outcome::Gone => {
                // Deleted (or deleted and recreated) while booting: don't leak
                // this incarnation's container or volume.
                runtime.retire(&key, &spec).await;
                return;
            }
        }
    }
    if let Some(hook) = persist {
        hook().await;
    }
}

fn settle(
    state: &SharedOpenSearchState,
    account: &str,
    name: &str,
    incarnation: &str,
    f: impl FnOnce(&mut Domain),
) {
    let mut accounts = state.write();
    if let Some(d) = accounts
        .get_mut(account)
        .and_then(|st| st.domains.get_mut(name))
        .filter(|d| d.incarnation() == incarnation)
    {
        f(d);
    }
}

/// Tear down a removed domain's cluster: its container and data volume.
/// `domain` is the row as it was removed.
pub fn spawn_teardown(runtime: &Arc<OpenSearchRuntime>, domain: &Domain) {
    let rt = runtime.clone();
    let key = domain.arn.clone();
    let volume = domain_spec(domain).map(|s| s.volume);
    // Untrack now, under the caller's state lock, so a recreate under the
    // same name never reuses the doomed container.
    let cid = rt.untrack(&key);
    tokio::spawn(async move {
        if let Some(cid) = cid {
            rt.remove_container(&cid).await;
        }
        if let Some(v) = volume {
            rt.remove_volume(&v).await;
        }
    });
}
