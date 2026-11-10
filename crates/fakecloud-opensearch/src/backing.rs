//! Keeps each domain's backing search cluster in line with its configuration.
//!
//! One routine, [`converge_domain`], serves every path that can change what a
//! domain's container should be -- `CreateDomain`, `UpdateDomainConfig`,
//! `UpgradeDomain`, CloudFormation, restart recovery, the liveness watch --
//! so they cannot drift apart. It reads the domain's current [`DomainSpec`], asks the runtime for a
//! container started from it, and settles the domain: `Processing` clears and
//! the endpoint becomes reachable, or the bring-up failure is recorded and the
//! domain degrades to control plane only. A spec that changed while the
//! container was booting is converged again before settling.

use std::sync::Arc;
use std::time::Duration;

use fakecloud_persistence::SnapshotHook;

use crate::runtime::{DomainSpec, Liveness, OpenSearchRuntime};
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
    if domain.processing.is_some() {
        // A bring-up is in flight. It re-reads the spec before settling, so
        // it picks a change up either way; only a change of what it is
        // starting relabels the status and queues another pass. A bring-up
        // not yet started reads the latest spec when it does.
        return match rt.inflight_spec(&domain.arn) {
            Some(inflight) if inflight != spec => {
                domain.processing = Some(status.to_string());
                true
            }
            _ => false,
        };
    }
    // A dead cluster (found by the liveness watch) counts as changed, so a
    // configuration change brings it back.
    let unchanged = domain.data_plane.is_some()
        && rt.tracked_spec(&domain.arn) == Some(spec)
        && rt.tracked_alive(&domain.arn);
    if unchanged {
        return false;
    }
    begin(true, domain, status)
}

/// Bring `account`'s domain `name` up from its current spec and settle it.
/// `persist` writes the snapshot once the domain settles.
pub async fn converge_domain(
    state: SharedOpenSearchState,
    runtime: Arc<OpenSearchRuntime>,
    account: String,
    name: String,
    persist: Option<SnapshotHook>,
) {
    loop {
        let target = {
            let accounts = state.read();
            let target = accounts
                .get(&account)
                .and_then(|st| st.domains.get(&name))
                .filter(|d| !d.deleted)
                .map(|d| (d.arn.clone(), d.incarnation(), domain_spec(d)));
            // Registered under the lock the spec was read under, so a
            // configuration change (made under the write lock) sees it.
            if let Some((key, _, Some(spec))) = &target {
                runtime.begin_inflight(key, spec);
            }
            target
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
        let result = runtime.ensure_domain(&key, &spec).await;
        // Settle under the lock, unless the domain moved on meanwhile.
        enum Outcome {
            Settled,
            Again,
            Gone,
        }
        let outcome = {
            let mut accounts = state.write();
            runtime.end_inflight(&key);
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

/// Tear down a removed domain's cluster: its container, any untracked
/// leftover, and its data volume. `domain` is the row as it was removed.
pub fn spawn_teardown(runtime: &Arc<OpenSearchRuntime>, domain: &Domain) {
    let rt = runtime.clone();
    let key = domain.arn.clone();
    let volume = domain_spec(domain).map(|s| s.volume);
    // Untrack now, under the caller's state lock, so a recreate under the
    // same name never reuses the doomed container.
    let cid = rt.untrack(&key);
    tokio::spawn(async move {
        rt.teardown(&key, cid.as_deref(), volume.as_deref()).await;
    });
}

/// How often the liveness watch probes running clusters.
pub const LIVENESS_INTERVAL: Duration = Duration::from_secs(30);
/// Consecutive unanswered probes of a running container before its node
/// counts as wedged. A dead container counts at once.
pub const UNRESPONSIVE_LIMIT: u32 = 3;

/// Probe every settled domain's cluster once. A dead one (container gone or
/// stopped, never started by this process, or a node unanswered
/// [`UNRESPONSIVE_LIMIT`] probes in a row) loses its endpoint binding, goes
/// back to `Processing` (`Modifying`) and is returned for a
/// [`converge_domain`]. `misses` carries the unanswered-probe counts between
/// passes. No lock is held across an await.
pub async fn check_liveness(
    state: &SharedOpenSearchState,
    runtime: &OpenSearchRuntime,
    misses: &mut std::collections::HashMap<String, u32>,
) -> Vec<(String, String)> {
    struct Live {
        account: String,
        name: String,
        arn: String,
        incarnation: String,
        container_id: String,
    }
    let live: Vec<Live> = {
        let accounts = state.read();
        accounts
            .iter()
            .flat_map(|(account, st)| {
                st.domains.values().filter_map(move |d| {
                    let dp = d.data_plane.as_ref()?;
                    (!d.deleted && d.processing.is_none()).then(|| Live {
                        account: account.to_string(),
                        name: d.name.clone(),
                        arn: d.arn.clone(),
                        incarnation: d.incarnation(),
                        container_id: dp.container_id.clone(),
                    })
                })
            })
            .collect()
    };
    misses.retain(|arn, _| live.iter().any(|l| &l.arn == arn));
    let mut dead = Vec::new();
    for l in live {
        let gone = match runtime.liveness(&l.arn).await {
            Liveness::Healthy => {
                misses.remove(&l.arn);
                continue;
            }
            Liveness::Dead | Liveness::Untracked => true,
            Liveness::Unresponsive => {
                let n = misses.entry(l.arn.clone()).or_default();
                *n += 1;
                *n >= UNRESPONSIVE_LIMIT
            }
        };
        if !gone {
            continue;
        }
        misses.remove(&l.arn);
        runtime.mark_dead(&l.arn, &l.container_id);
        let mut accounts = state.write();
        if let Some(d) = accounts
            .get_mut(&l.account)
            .and_then(|st| st.domains.get_mut(&l.name))
            .filter(|d| {
                !d.deleted
                    && d.incarnation() == l.incarnation
                    && d.processing.is_none()
                    && d.data_plane.as_ref().map(|dp| dp.container_id.as_str())
                        == Some(l.container_id.as_str())
            })
        {
            tracing::warn!(domain = %l.arn, container_id = %l.container_id, "search cluster is gone; bringing it back");
            d.data_plane = None;
            d.processing = Some(MODIFYING.to_string());
            dead.push((l.account, l.name));
        }
    }
    dead
}

/// Run [`check_liveness`] every [`LIVENESS_INTERVAL`], converging every dead
/// domain it finds. Runs for the life of the process.
pub async fn watch_liveness(
    state: SharedOpenSearchState,
    runtime: Arc<OpenSearchRuntime>,
    persist: Option<SnapshotHook>,
) {
    let mut misses = std::collections::HashMap::new();
    loop {
        tokio::time::sleep(LIVENESS_INTERVAL).await;
        for (account, name) in check_liveness(&state, &runtime, &mut misses).await {
            tokio::spawn(converge_domain(
                state.clone(),
                runtime.clone(),
                account,
                name,
                persist.clone(),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::fake::FakeEngine;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    const ACCOUNT: &str = "123456789012";

    fn state_with(name: &str, engine: &str) -> SharedOpenSearchState {
        let d: Domain = serde_json::from_value(serde_json::json!({
            "name": name,
            "domain_id": format!("{ACCOUNT}/{name}"),
            "arn": format!("arn:aws:es:us-east-1:{ACCOUNT}:domain/{name}"),
            "engine_version": engine,
            "created_via_es": false,
            "endpoint": format!("search-{name}-x.us-east-1.es.amazonaws.com"),
            "created": false,
            "deleted": false,
            "config": {},
            "tags": {},
            "created_at": "2026-01-01T00:00:00Z",
        }))
        .unwrap();
        let state: SharedOpenSearchState = Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "",
        )));
        state
            .write()
            .get_or_create(ACCOUNT)
            .domains
            .insert(name.to_string(), d);
        state
    }

    fn with_domain<R>(
        state: &SharedOpenSearchState,
        name: &str,
        f: impl FnOnce(&mut Domain) -> R,
    ) -> R {
        f(state
            .write()
            .get_or_create(ACCOUNT)
            .domains
            .get_mut(name)
            .unwrap())
    }

    async fn converge(state: &SharedOpenSearchState, engine: &FakeEngine, name: &str) {
        converge_domain(
            state.clone(),
            engine.runtime.clone(),
            ACCOUNT.into(),
            name.into(),
            None,
        )
        .await;
    }

    async fn settled(state: &SharedOpenSearchState, engine: &FakeEngine, name: &str) {
        assert!(with_domain(state, name, |d| begin(true, d, CREATING)));
        converge(state, engine, name).await;
        with_domain(state, name, |d| {
            assert!(d.processing.is_none());
            assert!(d.data_plane.is_some(), "{:?}", d.data_plane_error);
        });
    }

    /// A bring-up that fails after `create` removes the container it made,
    /// and a teardown reaps any untracked leftover of the domain before the
    /// data volume, so the volume is never left mounted.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_bring_up_and_its_teardown_leave_no_container_or_volume() {
        let engine = FakeEngine::start().await;
        let state = state_with("broken", "OpenSearch_2.11");
        engine.flag("fail-start", true);
        assert!(with_domain(&state, "broken", |d| begin(true, d, CREATING)));
        converge(&state, &engine, "broken").await;
        let (arn, volume) = with_domain(&state, "broken", |d| {
            assert!(d.processing.is_none() && d.data_plane.is_none());
            assert!(d.data_plane_error.as_deref().unwrap().contains("boom"));
            (d.arn.clone(), domain_spec(d).unwrap().volume)
        });
        let calls = engine.calls();
        assert!(calls.iter().any(|c| c == "rm -f cid1"), "{calls:#?}");
        assert!(engine.runtime.tracked_domains().is_empty());

        // A leftover the runtime doesn't track (a timed-out create) is reaped
        // by the teardown, matched on this process's owner label.
        engine.set_ps("leftover1\n");
        let removed = state
            .write()
            .get_or_create(ACCOUNT)
            .domains
            .remove("broken")
            .unwrap();
        spawn_teardown(&engine.runtime, &removed);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let calls = engine.calls();
            let reaped = calls.iter().position(|c| c == "rm -f leftover1");
            let vol = calls
                .iter()
                .position(|c| c == &format!("volume rm -f {volume}"));
            if let (Some(r), Some(v)) = (reaped, vol) {
                assert!(r < v, "leftover reaped before the volume: {calls:#?}");
                let ps = calls.iter().find(|c| c.starts_with("ps -aq")).unwrap();
                assert!(
                    ps.contains(&format!("label=fakecloud-opensearch={arn}")),
                    "{ps}"
                );
                assert!(
                    ps.contains(&format!(
                        "label=fakecloud-opensearch-owner={}",
                        engine.owner_id()
                    )),
                    "{ps}"
                );
                assert!(!ps.contains("fakecloud-instance"), "{ps}");
                break;
            }
            assert!(std::time::Instant::now() < deadline, "{calls:#?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The owner label is random per runtime, so two processes (both pid 1 in
    /// their containers) sharing a daemon never reap each other's clusters.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_runtime_has_its_own_owner_label() {
        let a = FakeEngine::start().await;
        let b = FakeEngine::start().await;
        assert_ne!(a.owner_id(), b.owner_id());
        let state = state_with("owned", "OpenSearch_2.11");
        settled(&state, &a, "owned").await;
        let create = a
            .calls()
            .into_iter()
            .find(|c| c.starts_with("create"))
            .unwrap();
        assert!(
            create.contains(&format!("fakecloud-opensearch-owner={}", a.owner_id())),
            "{create}"
        );
        assert!(
            create.contains(&format!(
                "fakecloud-instance=fakecloud-{}",
                std::process::id()
            )),
            "the reaper's pid label is kept: {create}"
        );
    }

    /// A cluster whose container died is noticed, loses its endpoint, goes
    /// back to `Modifying` and is brought back up; a configuration change
    /// also treats a dead tracked container as changed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dead_cluster_is_noticed_and_brought_back() {
        let engine = FakeEngine::start().await;
        let state = state_with("crashy", "OpenSearch_2.11");
        settled(&state, &engine, "crashy").await;
        let mut misses = std::collections::HashMap::new();
        assert!(check_liveness(&state, &engine.runtime, &mut misses)
            .await
            .is_empty());

        engine
            .healthy
            .store(false, std::sync::atomic::Ordering::SeqCst);
        engine.flag("dead", true);
        let dead = check_liveness(&state, &engine.runtime, &mut misses).await;
        assert_eq!(dead, vec![(ACCOUNT.to_string(), "crashy".to_string())]);
        with_domain(&state, "crashy", |d| {
            assert!(d.data_plane.is_none());
            assert_eq!(d.reachable_url(), None);
            assert_eq!(d.processing.as_deref(), Some(MODIFYING));
        });

        engine
            .healthy
            .store(true, std::sync::atomic::Ordering::SeqCst);
        engine.flag("dead", false);
        converge(&state, &engine, "crashy").await;
        with_domain(&state, "crashy", |d| {
            assert!(d.processing.is_none());
            assert_eq!(d.data_plane.as_ref().unwrap().container_id, "cid2");
        });

        // A dead tracked cluster makes an otherwise no-op change re-converge.
        assert!(!with_domain(&state, "crashy", |d| begin_change(
            Some(&engine.runtime),
            d,
            MODIFYING
        )));
        engine.runtime.mark_dead(
            &format!("arn:aws:es:us-east-1:{ACCOUNT}:domain/crashy"),
            "cid2",
        );
        assert!(with_domain(&state, "crashy", |d| begin_change(
            Some(&engine.runtime),
            d,
            MODIFYING
        )));
    }

    /// A node that runs but stops answering counts only after repeated
    /// misses.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unresponsive_node_needs_repeated_misses() {
        let engine = FakeEngine::start().await;
        let state = state_with("slow", "OpenSearch_2.11");
        settled(&state, &engine, "slow").await;
        engine
            .healthy
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let mut misses = std::collections::HashMap::new();
        for _ in 1..UNRESPONSIVE_LIMIT {
            assert!(check_liveness(&state, &engine.runtime, &mut misses)
                .await
                .is_empty());
        }
        assert_eq!(
            check_liveness(&state, &engine.runtime, &mut misses)
                .await
                .len(),
            1
        );
    }

    /// During a bring-up only a change of the spec being started relabels
    /// the status and asks for another pass.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_change_during_bring_up_reconverges_only_for_a_new_spec() {
        let engine = FakeEngine::start().await;
        let state = state_with("booting", "OpenSearch_2.11");
        engine
            .healthy
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(with_domain(&state, "booting", |d| begin(true, d, CREATING)));
        let task = tokio::spawn(converge_domain(
            state.clone(),
            engine.runtime.clone(),
            ACCOUNT.into(),
            "booting".into(),
            None,
        ));
        let arn = format!("arn:aws:es:us-east-1:{ACCOUNT}:domain/booting");
        while engine.runtime.inflight_spec(&arn).is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // A tag-like change: same spec.
        assert!(!with_domain(&state, "booting", |d| {
            d.config.insert(
                "ClusterConfig".into(),
                serde_json::json!({"InstanceCount": 2}),
            );
            begin_change(Some(&engine.runtime), d, MODIFYING)
        }));
        with_domain(&state, "booting", |d| {
            assert_eq!(d.processing.as_deref(), Some(CREATING))
        });
        // A node-setting change: new spec.
        assert!(with_domain(&state, "booting", |d| {
            d.config.insert(
                "AdvancedOptions".into(),
                serde_json::json!({"indices.query.bool.max_clause_count": "2048"}),
            );
            begin_change(Some(&engine.runtime), d, MODIFYING)
        }));
        with_domain(&state, "booting", |d| {
            assert_eq!(d.processing.as_deref(), Some(MODIFYING))
        });
        // Once the node answers, the bring-up settles on the latest spec.
        engine
            .healthy
            .store(true, std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(30), task)
            .await
            .unwrap()
            .unwrap();
        with_domain(&state, "booting", |d| {
            assert!(d.processing.is_none() && d.data_plane.is_some());
            assert_eq!(
                engine.runtime.tracked_spec(&d.arn),
                domain_spec(d),
                "settled on the spec with the new setting"
            );
        });
    }

    /// Shutting down stops a bring-up still waiting for its node, removes
    /// its container, and reaps whatever else this process started.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_removes_a_container_still_coming_up() {
        let engine = FakeEngine::start().await;
        let state = state_with("late", "OpenSearch_2.11");
        engine
            .healthy
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(with_domain(&state, "late", |d| begin(true, d, CREATING)));
        let task = tokio::spawn(converge_domain(
            state.clone(),
            engine.runtime.clone(),
            ACCOUNT.into(),
            "late".into(),
            None,
        ));
        while !engine.calls().iter().any(|c| c.starts_with("start")) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        engine.set_ps("orphan9\n");
        engine.runtime.stop_all().await;
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        let calls = engine.calls();
        assert!(calls.iter().any(|c| c == "rm -f cid1"), "{calls:#?}");
        assert!(calls.iter().any(|c| c == "rm -f orphan9"), "{calls:#?}");
        let creates = calls.iter().filter(|c| c.starts_with("create")).count();
        // A later bring-up refuses to start anything.
        converge(&state, &engine, "late").await;
        assert_eq!(
            engine
                .calls()
                .iter()
                .filter(|c| c.starts_with("create"))
                .count(),
            creates
        );
        with_domain(&state, "late", |d| assert!(d.data_plane.is_none()));
    }
}
