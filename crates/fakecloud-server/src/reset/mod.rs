//! The reset endpoints (`/_reset`, `/_fakecloud/reset/{service}`,
//! `/_fakecloud/reset/{service}/{account_id}`): clear service state, tear down
//! the backing containers, and in persistent mode write the reset state
//! through to disk so a restart does not bring it back.

mod admin;
mod explicit;
mod registry;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use fakecloud_persistence::{S3Store, SnapshotHook};
use fakecloud_sdk::types;

pub(crate) use admin::create_admin_in_account;
use explicit::{ExplicitService, EXPLICIT};
pub(crate) use registry::{reset_services, ResetServiceStates, ServiceReset};

// Make pub so main.rs can construct it
#[derive(Clone)]
pub(crate) struct ResetState {
    pub iam: fakecloud_iam::SharedIamState,
    pub sqs: fakecloud_sqs::SharedSqsState,
    pub sns: fakecloud_sns::SharedSnsState,
    pub eb: fakecloud_eventbridge::SharedEventBridgeState,
    pub ssm: fakecloud_ssm::SharedSsmState,
    pub dynamodb: fakecloud_dynamodb::SharedDynamoDbState,
    pub lambda: fakecloud_lambda::SharedLambdaState,
    pub secretsmanager: fakecloud_secretsmanager::SharedSecretsManagerState,
    pub s3: fakecloud_s3::SharedS3State,
    pub logs: fakecloud_logs::SharedLogsState,
    pub kms: fakecloud_kms::SharedKmsState,
    pub cloudformation: fakecloud_cloudformation::SharedCloudFormationState,
    pub ses: fakecloud_ses::SharedSesState,
    pub cognito: fakecloud_cognito::SharedCognitoState,
    pub kinesis: fakecloud_kinesis::SharedKinesisState,
    pub rds: fakecloud_rds::SharedRdsState,
    pub elasticache: fakecloud_elasticache::SharedElastiCacheState,
    pub ecr: fakecloud_ecr::SharedEcrState,
    pub ecs: fakecloud_ecs::SharedEcsState,
    pub stepfunctions: fakecloud_stepfunctions::SharedStepFunctionsState,
    pub scheduler: fakecloud_scheduler::SharedSchedulerState,
    pub apigatewayv1: fakecloud_apigateway::SharedApiGatewayState,
    pub apigatewayv2: fakecloud_apigatewayv2::SharedApiGatewayV2State,
    pub bedrock: fakecloud_bedrock::SharedBedrockState,
    pub bedrock_agent: fakecloud_bedrock_agent::SharedBedrockAgentState,
    pub bedrock_agent_runtime: fakecloud_bedrock_agent_runtime::SharedBedrockAgentRuntimeState,
    pub cloudfront: fakecloud_cloudfront::SharedCloudFrontState,
    pub route53: fakecloud_route53::SharedRoute53State,
    pub acm: fakecloud_acm::SharedAcmState,
    pub acmpca: fakecloud_acmpca::SharedAcmPcaState,
    pub config: fakecloud_config::SharedConfigState,
    pub route53resolver: fakecloud_route53resolver::SharedRoute53ResolverState,
    pub firehose: fakecloud_firehose::SharedFirehoseState,
    pub glue: fakecloud_glue::SharedGlueState,
    pub cloudwatch: fakecloud_cloudwatch::SharedCloudWatchState,
    pub application_autoscaling:
        fakecloud_application_autoscaling::SharedApplicationAutoScalingState,
    pub wafv2: fakecloud_wafv2::SharedWafv2State,
    pub athena: fakecloud_athena::SharedAthenaState,
    pub organizations: fakecloud_organizations::SharedOrganizationsState,
    pub servicequotas: fakecloud_servicequotas::SharedServiceQuotasState,
    pub servicequotas_settings: fakecloud_servicequotas::SharedQuotaSettings,
    /// The quota settings the server started with, which a reset restores.
    pub servicequotas_baseline: fakecloud_servicequotas::QuotaSettings,
    pub container_runtime: Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
    pub rds_runtime: Option<Arc<fakecloud_rds::runtime::RdsRuntime>>,
    pub elasticache_runtime: Option<Arc<fakecloud_elasticache::runtime::ElastiCacheRuntime>>,
    pub ecs_runtime: Option<Arc<fakecloud_ecs::runtime::EcsRuntime>>,
    pub ec2: fakecloud_ec2::SharedEc2State,
    pub ec2_runtime: Option<Arc<fakecloud_ec2::runtime::Ec2Runtime>>,
    /// Wiring that only exists once every service is built (the snapshot
    /// hooks, the S3 disk store, the services reset through a registered
    /// entry). Filled once by `main.rs` before the server starts serving.
    pub late: Arc<OnceLock<LateReset>>,
}

/// Why a reset request was refused.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ResetError {
    UnknownService(String),
    /// The service has no per-account state (Organizations).
    NoAccountReset(String),
}

impl ResetError {
    pub(crate) fn status(&self) -> axum::http::StatusCode {
        match self {
            Self::UnknownService(_) => axum::http::StatusCode::NOT_FOUND,
            Self::NoAccountReset(_) => axum::http::StatusCode::BAD_REQUEST,
        }
    }

    pub(crate) fn message(&self) -> String {
        match self {
            Self::UnknownService(service) => format!("Unknown service: {service}"),
            Self::NoAccountReset(service) => format!(
                "{service} has no per-account state to reset; reset it with \
                 /_fakecloud/reset/{service}"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot stores and their hooks.
// ---------------------------------------------------------------------------

/// The directories (under the data path) of every snapshot store the server
/// opened, recorded by [`snapshot_store_path`].
static SNAPSHOT_STORES: parking_lot::Mutex<BTreeSet<&'static str>> =
    parking_lot::Mutex::new(BTreeSet::new());

/// The snapshot file of the store in `dir` under `data_path`. Every snapshot
/// store `main.rs` opens goes through here, which records it, so the reset
/// wiring can be checked against the full set of stores.
pub(crate) fn snapshot_store_path(data_path: &Path, dir: &'static str) -> PathBuf {
    SNAPSHOT_STORES.lock().insert(dir);
    snapshot_file_path(data_path, dir)
}

/// The snapshot file of the store in `dir` under `data_path`, without
/// recording a store (for readers of an existing snapshot).
pub(crate) fn snapshot_file_path(data_path: &Path, dir: &str) -> PathBuf {
    data_path.join(dir).join("snapshot.json")
}

/// The persist-hook key of the service whose snapshot store lives in `dir`.
/// Hooks are keyed like the CloudFormation provisioner's, which names a few
/// services differently from their data directory.
pub(crate) fn hook_key_for_store(dir: &str) -> &str {
    match dir {
        "apigatewayv1" => "apigateway",
        "cognito-idp" => "cognito",
        "opensearch" => "es",
        "efs" => "elasticfilesystem",
        other => other,
    }
}

/// Every persist-hook key some reset writes.
fn reset_hook_keys(services: &[ServiceReset]) -> BTreeSet<&'static str> {
    EXPLICIT
        .iter()
        .flat_map(|e| e.hooks.iter().copied())
        .chain(services.iter().map(|s| s.hook))
        .collect()
}

/// What keeps a reset from reaching the disk: a snapshot store with no
/// persist hook, a store or hook no reset clears, a reset whose hook is not
/// registered. Empty when the wiring is complete.
pub(crate) fn wiring_gaps<'a>(
    stores: impl IntoIterator<Item = &'a str>,
    hooks: &BTreeSet<&'a str>,
    reset: &BTreeSet<&'a str>,
) -> Vec<String> {
    let mut gaps = Vec::new();
    for dir in stores {
        let key = hook_key_for_store(dir);
        if !hooks.contains(key) {
            gaps.push(format!(
                "snapshot store `{dir}` has no persist hook `{key}`"
            ));
        }
        if !reset.contains(key) {
            gaps.push(format!(
                "snapshot store `{dir}` is not cleared by any reset"
            ));
        }
    }
    for key in hooks.difference(reset) {
        gaps.push(format!("persist hook `{key}` is not written by any reset"));
    }
    for key in reset.difference(hooks) {
        gaps.push(format!(
            "reset writes persist hook `{key}`, which is not registered"
        ));
    }
    gaps
}

/// See [`ResetState::late`].
#[derive(Default)]
pub(crate) struct LateReset {
    /// Every snapshot-backed service's persist hook, keyed like the
    /// CloudFormation provisioner's (`cfn_snapshot_hooks`). Empty in memory
    /// mode.
    hooks: BTreeMap<&'static str, SnapshotHook>,
    services: Vec<ServiceReset>,
    /// The S3 store, whose buckets live on disk one directory each rather than
    /// in a snapshot.
    s3_store: Option<Arc<dyn S3Store>>,
}

impl LateReset {
    pub(crate) fn new(
        hooks: BTreeMap<&'static str, SnapshotHook>,
        services: Vec<ServiceReset>,
        s3_store: Option<Arc<dyn S3Store>>,
    ) -> Self {
        Self {
            hooks,
            services,
            s3_store,
        }
    }

    /// The persist hook registered under `key`.
    pub(crate) fn hook(&self, key: &str) -> Option<SnapshotHook> {
        self.hooks.get(key).cloned()
    }

    /// See [`wiring_gaps`], for the stores this process opened.
    pub(crate) fn gaps(&self) -> Vec<String> {
        let stores: Vec<&'static str> = SNAPSHOT_STORES.lock().iter().copied().collect();
        let hooks: BTreeSet<&'static str> = self.hooks.keys().copied().collect();
        wiring_gaps(stores, &hooks, &reset_hook_keys(&self.services))
    }
}

// ---------------------------------------------------------------------------
// Container teardown.
// ---------------------------------------------------------------------------

// A reset snapshots the reset rows' incarnation ids and volumes and clears
// the state under one write lock, then tears down by those ids. Runtime
// records are keyed by incarnation, so a resource created after the reset
// (even under a reset one's identifier) is never reached, and a start still in
// flight for a reset incarnation reaps itself once it finds its row gone.

/// `(DbiResourceId, data volume)` of every RDS instance in an account.
fn rds_incarnations(state: &fakecloud_rds::RdsState) -> Vec<(String, String)> {
    let tag = fakecloud_core::data_volume::current_scope().tag();
    state
        .instances
        .values()
        .map(|inst| {
            (
                inst.dbi_resource_id.clone(),
                inst.data_volume_name(tag, &state.account_id),
            )
        })
        .collect()
}

/// `(account, instance id)` of every EC2 instance in an account, whose
/// containers and data volumes a reset removes (stopped ones included).
fn ec2_instances(state: &fakecloud_ec2::Ec2State) -> Vec<(String, String)> {
    state
        .instances
        .keys()
        .map(|id| (state.account_id.clone(), id.clone()))
        .collect()
}

/// `(incarnation, data volume)` of every cache cluster, replication group and
/// serverless cache in an account (no volume for memcached).
fn elasticache_incarnations(
    state: &fakecloud_elasticache::ElastiCacheState,
) -> Vec<(String, Option<String>)> {
    let tag = fakecloud_core::data_volume::current_scope().tag();
    let account = &state.account_id;
    let clusters = state.cache_clusters.values().map(|c| {
        (
            c.incarnation(),
            (c.engine != "memcached").then(|| c.data_volume_name(tag, account)),
        )
    });
    let groups = state.replication_groups.values().map(|g| {
        (
            g.incarnation(),
            (g.engine != "memcached").then(|| g.data_volume_name(tag, account)),
        )
    });
    let serverless = state
        .serverless_caches
        .values()
        .map(|c| (c.incarnation(), Some(c.data_volume_name(tag, account))));
    clusters.chain(groups).chain(serverless).collect()
}

/// Collect `rows` from the accounts in scope (every account for `None`) and
/// reset them, all under the caller's one write lock.
fn take_and_reset<T, R>(
    mas: &mut fakecloud_core::multi_account::MultiAccountState<T>,
    account: Option<&str>,
    rows: impl Fn(&T) -> Vec<R>,
) -> Vec<R>
where
    T: fakecloud_core::multi_account::AccountState,
{
    match account {
        None => {
            let gone = mas.iter().flat_map(|(_, s)| rows(s)).collect();
            mas.reset();
            gone
        }
        Some(account) => {
            let gone = mas.get(account).map(&rows).unwrap_or_default();
            mas.reset_account(account);
            gone
        }
    }
}

/// How long a reset response waits for its container teardown.
const TEARDOWN_WAIT: std::time::Duration = std::time::Duration::from_secs(120);

/// Container and data-volume teardown a reset queued. The reset handlers
/// await it before replying, so once a reset returns, a resource recreated
/// under a reset one's identifier can't race the teardown and mount the old
/// data volume (or have its new container stopped).
///
/// It also carries the snapshot writes that put the reset on disk in
/// persistent mode, so a restart does not bring the reset state back.
#[derive(Default)]
pub(crate) struct Teardown {
    tasks: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
    /// The persist hooks queued, by key.
    persisted: Vec<&'static str>,
}

impl Teardown {
    pub(crate) fn push(&mut self, f: impl std::future::Future<Output = ()> + Send + 'static) {
        self.tasks.push(Box::pin(f));
    }

    /// Run the teardown on its own task (a client that hangs up mid-reset
    /// can't cancel it half way, leaving containers untracked and volumes
    /// behind) and wait for it, bounded so a wedged daemon can't hang the
    /// reset response; past the bound it keeps running in the background.
    pub(crate) async fn run(self) {
        // Each service's teardown runs on its own task, concurrently, so a
        // slow one can't eat the others' share of the wait.
        let tasks: Vec<_> = self.tasks.into_iter().map(tokio::spawn).collect();
        let deadline = tokio::time::Instant::now() + TEARDOWN_WAIT;
        for task in tasks {
            match tokio::time::timeout_at(deadline, task).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::error!(%err, "reset teardown failed; containers or volumes may be left behind");
                }
                Err(_) => {
                    tracing::warn!(
                        "reset teardown still running after {}s; continuing in the background",
                        TEARDOWN_WAIT.as_secs()
                    );
                }
            }
        }
    }
}

impl ResetState {
    /// The late wiring, or an empty one before `main.rs` filled it (unit
    /// tests, and a reset racing startup, which has nothing on disk to reach).
    fn late(&self) -> &LateReset {
        static EMPTY: OnceLock<LateReset> = OnceLock::new();
        self.late
            .get()
            .unwrap_or_else(|| EMPTY.get_or_init(LateReset::default))
    }

    /// Queue the persist hooks of `keys` on `teardown`, so the reset state is
    /// written through to disk before the reset replies. Each hook serializes
    /// behind its service's snapshot lock, so it lands after any write already
    /// in flight. Memory mode registers no hooks, so this queues nothing
    /// there.
    fn persist(&self, teardown: &mut Teardown, keys: impl IntoIterator<Item = &'static str>) {
        let late = self.late();
        for key in keys {
            let Some(hook) = late.hooks.get(key) else {
                continue;
            };
            if teardown.persisted.contains(&key) {
                continue;
            }
            teardown.persisted.push(key);
            teardown.push(hook());
        }
    }

    /// Reset IAM in `account` (every account for `None`). The reset drops the
    /// execution-role sessions warm Lambda instances hold: stop handing them
    /// invocations in the same step, then tear the free ones down in the
    /// background.
    fn reset_iam(&self, account: Option<&str>) {
        if let Some(ref rt) = self.container_runtime {
            rt.mark_credentials_revoked(account);
        }
        {
            let mut mas = self.iam.write();
            match account {
                None => mas.reset(),
                Some(account) => {
                    let region = mas.region().to_string();
                    if let Some(state) = mas.get_mut(account) {
                        state.reset(&region);
                    }
                }
            }
        }
        if let Some(ref rt) = self.container_runtime {
            let rt = rt.clone();
            tokio::spawn(async move { rt.retire_released().await });
        }
    }

    /// Reset Lambda in `account` (every account for `None`), with the warm
    /// instances of its functions.
    ///
    /// The state lock is released before the runtime's instance map is
    /// taken: the runtime nests them the other way round (instances, then
    /// state, in `list_warm_containers`), so holding both here could
    /// deadlock. This is the order `DeleteFunction` uses too. A function
    /// created and warmed in the gap loses that warm instance and cold-starts
    /// on its next invoke.
    fn reset_lambda(&self, account: Option<&str>, teardown: &mut Teardown) {
        {
            let mut mas = self.lambda.write();
            match account {
                None => mas.reset(),
                Some(account) => {
                    if let Some(state) = mas.get_mut(account) {
                        // Every region of the account.
                        state.clear();
                    }
                }
            }
        }
        if let Some(rt) = self.container_runtime.clone() {
            let taken = rt.take_account_instances(account);
            if !taken.is_empty() {
                teardown.push(async move { rt.terminate_taken(taken).await });
            }
        }
    }

    /// Reset ECS in `account` (every account for `None`), stopping the
    /// containers of the tasks it drops.
    fn reset_ecs(&self, account: Option<&str>, teardown: &mut Teardown) {
        let tasks = take_and_reset(&mut self.ecs.write(), account, |s| {
            s.tasks.keys().cloned().collect()
        });
        if let Some(rt) = self.ecs_runtime.clone() {
            if !tasks.is_empty() {
                // Each `stop_task` waits out its containers' stop timeout:
                // stop them all at once.
                teardown.push(async move {
                    let mut stops = tokio::task::JoinSet::new();
                    for task in tasks {
                        let rt = rt.clone();
                        stops.spawn(async move { rt.stop_task(&task, "fakecloud reset").await });
                    }
                    stops.join_all().await;
                });
            }
        }
    }

    /// Reset RDS in `account` (every account for `None`), stopping the
    /// backing containers and dropping the instances' data volumes: the
    /// instances are gone for good, so one recreated under the same
    /// identifier must start clean (the volumes would otherwise outlive the
    /// state, #2630).
    fn reset_rds(&self, account: Option<&str>, teardown: &mut Teardown) {
        let gone = take_and_reset(&mut self.rds.write(), account, rds_incarnations);
        if let Some(rt) = self.rds_runtime.clone() {
            teardown.push(async move {
                for (incarnation, volume) in gone {
                    rt.stop(&incarnation).await;
                    rt.remove_data_volume_named(&volume).await;
                }
            });
        }
    }

    /// Reset ElastiCache in `account` (every account for `None`), stopping
    /// the backing containers and dropping the resources' data volumes (see
    /// [`Self::reset_rds`]).
    fn reset_elasticache(&self, account: Option<&str>, teardown: &mut Teardown) {
        let gone = take_and_reset(
            &mut self.elasticache.write(),
            account,
            elasticache_incarnations,
        );
        if let Some(rt) = self.elasticache_runtime.clone() {
            teardown.push(async move {
                for (incarnation, volume) in gone {
                    rt.stop(&incarnation).await;
                    if let Some(volume) = volume {
                        rt.remove_data_volume_named(&volume).await;
                    }
                }
            });
        }
    }

    /// Reset EC2 in `account` (every account for `None`), tearing down every
    /// instance's container and data volume by instance id (see
    /// [`Self::reset_rds`]).
    fn reset_ec2(&self, account: Option<&str>, teardown: &mut Teardown) {
        let gone = take_and_reset(&mut self.ec2.write(), account, |rs| {
            rs.regions().flat_map(|(_, s)| ec2_instances(s)).collect()
        });
        if let Some(rt) = self.ec2_runtime.clone() {
            teardown.push(async move { rt.remove_instances(gone).await });
        }
    }

    /// Reset S3 in `account` (every account for `None`), deleting the reset
    /// buckets from the store. Under the S3 write lock each bucket directory
    /// is only renamed aside (fast and atomic), so a `CreateBucket` reusing a
    /// reset name, which waits for the lock, never writes into what is being
    /// removed; the recursive removal runs in the teardown, off the lock.
    fn reset_s3(&self, account: Option<&str>, teardown: &mut Teardown) {
        let detached: Vec<PathBuf> = {
            let mut mas = self.s3.write();
            let buckets: Vec<String> = match account {
                None => {
                    let buckets = mas
                        .iter()
                        .flat_map(|(_, s)| s.buckets.keys().cloned())
                        .collect();
                    // The recreated default account must keep the shared
                    // runtime wiring (body cache, write side effects) the
                    // server installed; it is not account data.
                    let mut runtime = fakecloud_s3::S3State::new("", "");
                    fakecloud_core::multi_account::AccountState::inherit_from(
                        &mut runtime,
                        mas.default_ref(),
                    );
                    mas.reset();
                    fakecloud_core::multi_account::AccountState::inherit_from(
                        mas.default_mut(),
                        &runtime,
                    );
                    buckets
                }
                Some(account) => match mas.get_mut(account) {
                    Some(state) => {
                        let buckets = state.buckets.keys().cloned().collect();
                        state.reset();
                        buckets
                    }
                    None => Vec::new(),
                },
            };
            let Some(store) = self.late().s3_store.as_ref() else {
                return;
            };
            buckets
                .iter()
                .filter_map(|bucket| match store.detach_bucket(bucket) {
                    Ok(path) => path,
                    Err(err) => {
                        tracing::error!(%bucket, %err, "reset could not remove the bucket from disk");
                        None
                    }
                })
                .collect()
        };
        if detached.is_empty() {
            return;
        }
        teardown.push(async move {
            let removed = tokio::task::spawn_blocking(move || {
                for path in detached {
                    if let Err(err) = fakecloud_persistence::s3::remove_detached(&path) {
                        tracing::error!(path = %path.display(), %err, "reset could not delete a detached bucket");
                    }
                }
            })
            .await;
            if let Err(err) = removed {
                tracing::error!(%err, "reset bucket deletion task panicked");
            }
        });
    }

    /// Reset Service Quotas: every account's applied values, requests and
    /// overrides, and the enforcement settings back to the startup flags.
    fn reset_servicequotas(&self) {
        self.servicequotas.write().reset();
        *self.servicequotas_settings.write() = self.servicequotas_baseline.clone();
    }

    /// The table row or registered entry `service` names.
    fn find(&self, service: &str) -> Option<Target<'_>> {
        if let Some(e) = EXPLICIT.iter().find(|e| e.names.contains(&service)) {
            return Some(Target::Explicit(e));
        }
        self.late()
            .services
            .iter()
            .find(|s| s.names.contains(&service))
            .map(Target::Registered)
    }

    pub(crate) fn reset_service(&self, service: &str) -> Result<Teardown, ResetError> {
        let target = self
            .find(service)
            .ok_or_else(|| ResetError::UnknownService(service.to_string()))?;
        let mut teardown = Teardown::default();
        match target {
            Target::Explicit(e) => {
                (e.reset_all)(self, &mut teardown);
                self.persist(&mut teardown, e.hooks.iter().copied());
            }
            Target::Registered(s) => {
                (s.reset_all)(&mut teardown);
                self.persist(&mut teardown, [s.hook]);
            }
        }
        tracing::info!(service = %service, "service state reset via per-service reset API");
        Ok(teardown)
    }

    /// Reset a single service's state for a specific account only.
    pub(crate) fn reset_service_for_account(
        &self,
        service: &str,
        account_id: &str,
    ) -> Result<Teardown, ResetError> {
        let target = self
            .find(service)
            .ok_or_else(|| ResetError::UnknownService(service.to_string()))?;
        let mut teardown = Teardown::default();
        match target {
            Target::Explicit(e) => {
                let reset = e
                    .reset_account
                    .ok_or_else(|| ResetError::NoAccountReset(service.to_string()))?;
                reset(self, account_id, &mut teardown);
                self.persist(&mut teardown, e.hooks.iter().copied());
            }
            Target::Registered(s) => {
                (s.reset_account)(account_id, &mut teardown);
                self.persist(&mut teardown, [s.hook]);
            }
        }
        tracing::info!(service = %service, account_id = %account_id, "service state reset for account via per-account reset API");
        Ok(teardown)
    }

    pub(crate) fn reset(&self) -> (axum::Json<types::ResetResponse>, Teardown) {
        let mut teardown = Teardown::default();
        let late = self.late();
        for e in EXPLICIT.iter().filter(|e| e.in_full) {
            (e.reset_all)(self, &mut teardown);
        }
        for s in &late.services {
            (s.reset_all)(&mut teardown);
        }
        self.persist(&mut teardown, reset_hook_keys(&late.services));
        tracing::info!("state reset via reset API");
        (
            axum::Json(types::ResetResponse {
                status: "ok".to_string(),
            }),
            teardown,
        )
    }
}

/// What a service name resolves to.
enum Target<'a> {
    Explicit(&'static ExplicitService),
    Registered(&'a ServiceReset),
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use chrono::Utc;
    use fakecloud_rds::{DbInstance, RdsState};

    use super::*;

    fn test_state() -> ResetState {
        ResetState {
            iam: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            sqs: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            sns: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            eb: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ssm: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            dynamodb: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            lambda: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            secretsmanager: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            s3: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            logs: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            kms: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            cloudformation: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            ses: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            cognito: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            kinesis: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            rds: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            elasticache: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ecr: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            ecs: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            stepfunctions: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            scheduler: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            apigatewayv1: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            apigatewayv2: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            bedrock: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "http://localhost:4566",
                ),
            )),
            bedrock_agent: Arc::new(parking_lot::RwLock::new(
                fakecloud_bedrock_agent::BedrockAgentAccounts::new(),
            )),
            bedrock_agent_runtime: Arc::new(parking_lot::RwLock::new(
                fakecloud_bedrock_agent_runtime::BedrockAgentRuntimeAccounts::new(),
            )),
            cloudfront: Arc::new(parking_lot::RwLock::new(
                fakecloud_cloudfront::CloudFrontAccounts::new(),
            )),
            route53: Arc::new(parking_lot::RwLock::new(
                fakecloud_route53::Route53Accounts::new(),
            )),
            acm: Arc::new(parking_lot::RwLock::new(fakecloud_acm::AcmAccounts::new())),
            acmpca: Arc::new(parking_lot::RwLock::new(
                fakecloud_acmpca::AcmPcaAccounts::new(),
            )),
            config: Arc::new(parking_lot::RwLock::new(
                fakecloud_config::ConfigAccounts::new(),
            )),
            route53resolver: Arc::new(parking_lot::RwLock::new(
                fakecloud_route53resolver::Route53ResolverAccounts::new(),
            )),
            firehose: Arc::new(parking_lot::RwLock::new(
                fakecloud_firehose::FirehoseAccounts::new(),
            )),
            glue: Arc::new(parking_lot::RwLock::new(fakecloud_glue::GlueAccounts::new())),
            cloudwatch: Arc::new(parking_lot::RwLock::new(
                fakecloud_cloudwatch::CloudWatchAccounts::new(),
            )),
            application_autoscaling: Arc::new(parking_lot::RwLock::new(
                fakecloud_application_autoscaling::ApplicationAutoScalingAccounts::new(),
            )),
            wafv2: Arc::new(parking_lot::RwLock::new(
                fakecloud_wafv2::Wafv2Accounts::new(),
            )),
            athena: Arc::new(parking_lot::RwLock::new(
                fakecloud_athena::AthenaAccounts::new(),
            )),
            organizations: Arc::new(parking_lot::RwLock::new(
                fakecloud_organizations::OrganizationsRegistry::default(),
            )),
            servicequotas: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            servicequotas_settings: Arc::new(parking_lot::RwLock::new(
                fakecloud_servicequotas::QuotaSettings::default(),
            )),
            servicequotas_baseline: fakecloud_servicequotas::QuotaSettings {
                enforce_all: true,
                ..Default::default()
            },
            container_runtime: None,
            rds_runtime: None,
            elasticache_runtime: None,
            ecs_runtime: None,
            ec2: Arc::new(parking_lot::RwLock::new(
                fakecloud_core::multi_account::MultiAccountState::new(
                    "123456789012",
                    "us-east-1",
                    "",
                ),
            )),
            ec2_runtime: None,
            late: Arc::new(OnceLock::new()),
        }
    }

    #[test]
    fn reset_service_clears_rds_state() {
        let mut rds_mas: fakecloud_core::multi_account::MultiAccountState<RdsState> =
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "");
        let rds = rds_mas.default_mut();
        let created_at = Utc::now();
        rds.instances.insert(
            "db-1".to_string(),
            DbInstance {
                associated_roles: Vec::new(),
                db_instance_identifier: "db-1".to_string(),
                db_instance_arn: "arn:aws:rds:us-east-1:123456789012:db:db-1".to_string(),
                db_instance_class: "db.t3.micro".to_string(),
                engine: "postgres".to_string(),
                engine_version: "16.3".to_string(),
                db_instance_status: "available".to_string(),
                master_username: "admin".to_string(),
                db_name: Some("postgres".to_string()),
                db_subnet_group_name: None,
                endpoint_address: "127.0.0.1".to_string(),
                port: 5432,
                allocated_storage: 20,
                publicly_accessible: true,
                deletion_protection: false,
                created_at,
                dbi_resource_id: "db-test".to_string(),
                master_user_password: "secret123".to_string(),
                container_id: "container-id".to_string(),
                host_port: 15432,
                data_volume: None,
                tags: Vec::new(),
                read_replica_source_db_instance_identifier: None,
                read_replica_db_instance_identifiers: Vec::new(),
                vpc_security_group_ids: Vec::new(),
                db_parameter_group_name: None,
                backup_retention_period: 1,
                preferred_backup_window: "03:00-04:00".to_string(),
                preferred_maintenance_window: None,
                latest_restorable_time: Some(created_at),
                option_group_name: None,
                multi_az: false,
                pending_modified_values: None,
                availability_zone: None,
                storage_type: None,
                storage_encrypted: false,
                kms_key_id: None,
                iam_database_authentication_enabled: false,
                iops: None,
                monitoring_interval: None,
                monitoring_role_arn: None,
                performance_insights_enabled: false,
                performance_insights_kms_key_id: None,
                performance_insights_retention_period: None,
                enabled_cloudwatch_logs_exports: Vec::new(),
                ca_certificate_identifier: None,
                network_type: None,
                character_set_name: None,
                auto_minor_version_upgrade: None,
                copy_tags_to_snapshot: None,
                master_user_secret_arn: None,
                master_user_secret_kms_key_id: None,
                license_model: None,
                max_allocated_storage: None,
                multi_tenant: None,
                storage_throughput: None,
                tde_credential_arn: None,
                delete_automated_backups: None,
                db_security_groups: Vec::new(),
                domain: None,
                domain_fqdn: None,
                domain_ou: None,
                domain_iam_role_name: None,
                domain_auth_secret_arn: None,
                domain_dns_ips: Vec::new(),
                db_cluster_identifier: None,
                activity_stream: None,
            },
        );

        let state = ResetState {
            rds: Arc::new(parking_lot::RwLock::new(rds_mas)),
            ..test_state()
        };

        // Service Quotas: a reset drops applied values and restores the
        // startup enforcement settings.
        state
            .servicequotas
            .write()
            .get_or_create("123456789012")
            .applied
            .insert("us-east-1|vpc|L-2AFB9258".into(), 1.0);
        state.servicequotas_settings.write().enforce_all = false;
        state
            .reset_service("servicequotas")
            .expect("reset servicequotas");
        assert!(state
            .servicequotas
            .read()
            .get("123456789012")
            .is_some_and(|d| d.applied.is_empty()));
        assert!(state.servicequotas_settings.read().enforce_all);
        // A per-account reset keeps the organization template marker, so the
        // template is not applied to the account a second time.
        {
            let mut sq = state.servicequotas.write();
            let data = sq.get_or_create("123456789012");
            data.template_checked = Some("111111111111@marker".into());
            data.applied.insert("us-east-1|vpc|L-2AFB9258".into(), 9.0);
        }
        state
            .reset_service_for_account("servicequotas", "123456789012")
            .expect("reset servicequotas for account");
        {
            let sq = state.servicequotas.read();
            let data = sq.get("123456789012").unwrap();
            assert!(data.applied.is_empty());
            assert_eq!(
                data.template_checked.as_deref(),
                Some("111111111111@marker")
            );
        }

        state.reset_service("ec2").expect("reset ec2");
        state.reset_service("rds").expect("reset rds");

        assert!(state.rds.read().default_ref().instances.is_empty());
    }

    type Counts = Arc<parking_lot::Mutex<BTreeMap<&'static str, usize>>>;

    fn counting_hook(counts: &Counts, key: &'static str) -> SnapshotHook {
        let counts = counts.clone();
        Arc::new(move || {
            let counts = counts.clone();
            Box::pin(async move {
                *counts.lock().entry(key).or_default() += 1;
            })
        })
    }

    #[tokio::test]
    async fn firehose_reset_invalidates_old_generation_and_preserves_other_account() {
        use fakecloud_firehose::{DeliveryStream, DrainReport, FirehoseEngine};
        use fakecloud_persistence::BodyRef;
        const FIRST: &str = "123456789012";
        const OTHER: &str = "222222222222";
        for mode in ["account", "service", "all"] {
            let state = test_state();
            let (late, _) = counting_late(swf_state());
            assert!(state.late.set(late).is_ok());
            let insert_stream = |account: &str| {
                let mut stream: DeliveryStream = serde_json::from_value(serde_json::json!({
                    "name": "events", "arn": "arn", "status": "ACTIVE", "stream_type": "DirectPut",
                    "created_at": "2026-10-12T00:00:00Z", "last_update": "2026-10-12T00:00:00Z",
                    "version_id": "1", "destination": null, "tags": {}
                }))
                .unwrap();
                stream.destination =
                    fakecloud_firehose::config::parse_s3_destination(&serde_json::json!({
                        "RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket",
                        "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0}
                    }))
                    .unwrap();
                let id = stream.instance_id;
                state
                    .firehose
                    .write()
                    .get_or_create(account, "us-east-1")
                    .streams_mut("us-east-1")
                    .insert("events".into(), stream);
                id
            };
            let insert_bucket = |account: &str| {
                state.s3.write().get_or_create(account).buckets.insert(
                    "bucket".into(),
                    fakecloud_s3::S3Bucket::new("bucket", "us-east-1", "owner"),
                );
            };
            let old_id = insert_stream(FIRST);
            insert_stream(OTHER);
            insert_bucket(FIRST);
            insert_bucket(OTHER);
            let engine = Arc::new(FirehoseEngine::new(state.firehose.clone()));
            engine.set_s3(state.s3.clone());
            engine
                .enqueue(
                    FIRST,
                    "us-east-1",
                    "events",
                    vec![bytes::Bytes::from_static(b"stale")],
                )
                .unwrap();
            engine
                .enqueue(
                    OTHER,
                    "us-east-1",
                    "events",
                    vec![bytes::Bytes::from_static(b"other")],
                )
                .unwrap();
            let teardown = match mode {
                "account" => state.reset_service_for_account("firehose", FIRST).unwrap(),
                "service" => state.reset_service("firehose").unwrap(),
                _ => state.reset().1,
            };
            // Synchronous reset invalidates eligibility before async teardown.
            assert!(state.firehose.read().get(FIRST).is_none());
            let new_id = insert_stream(FIRST);
            assert_ne!(old_id, new_id);
            if mode == "all" {
                insert_bucket(FIRST);
                insert_bucket(OTHER);
            }
            teardown.run().await;
            engine
                .enqueue(
                    FIRST,
                    "us-east-1",
                    "events",
                    vec![bytes::Bytes::from_static(b"new")],
                )
                .unwrap();
            assert_eq!(
                engine.shutdown().await,
                DrainReport {
                    delivered_batches: if mode == "account" { 2 } else { 1 },
                    undelivered_records: 0,
                    undelivered_bytes: 0,
                }
            );
            let s3 = state.s3.read();
            let first = &s3.get(FIRST).unwrap().buckets["bucket"].objects;
            assert_eq!(first.len(), 1);
            let BodyRef::Memory(body) = &first.values().next().unwrap().body else {
                panic!("memory fixture")
            };
            assert_eq!(body.as_ref(), b"new");
            let other = &s3.get(OTHER).unwrap().buckets["bucket"].objects;
            if mode == "account" {
                assert_eq!(other.len(), 1);
                let BodyRef::Memory(body) = &other.values().next().unwrap().body else {
                    panic!("memory fixture")
                };
                assert_eq!(body.as_ref(), b"other");
            } else {
                assert!(other.is_empty());
            }
        }
    }

    /// A late wiring with SWF registered as a service reset through an entry,
    /// and a counting hook for every key a reset writes.
    fn counting_late(swf: fakecloud_swf::SharedSwfState) -> (LateReset, Counts) {
        let counts: Counts = Arc::default();
        let services = vec![ServiceReset::multi_account(&["swf"], "swf", swf)];
        let hooks = reset_hook_keys(&services)
            .into_iter()
            .map(|key| (key, counting_hook(&counts, key)))
            .collect();
        let late = LateReset {
            hooks,
            services,
            s3_store: None,
        };
        (late, counts)
    }

    fn swf_state() -> fakecloud_swf::SharedSwfState {
        Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn add_domain(swf: &fakecloud_swf::SharedSwfState, account: &str) {
        swf.write().get_or_create(account).domains.insert(
            "d".to_string(),
            fakecloud_swf::state::Domain {
                name: "d".to_string(),
                status: "REGISTERED".to_string(),
                description: None,
                retention_days: "1".to_string(),
                arn: "arn".to_string(),
            },
        );
    }

    fn domains(swf: &fakecloud_swf::SharedSwfState, account: &str) -> usize {
        swf.read().get(account).map_or(0, |d| d.domains.len())
    }

    #[tokio::test]
    async fn per_service_reset_persists_that_service() {
        let state = test_state();
        let (late, counts) = counting_late(swf_state());
        assert!(state.late.set(late).is_ok());

        let teardown = state.reset_service("events").unwrap();
        assert_eq!(teardown.persisted, vec!["eventbridge"]);
        teardown.run().await;
        let teardown = state.reset_service("apigateway").unwrap();
        assert_eq!(teardown.persisted, vec!["apigateway", "apigatewayv2"]);
        teardown.run().await;
        let teardown = state
            .reset_service_for_account("sqs", "123456789012")
            .unwrap();
        assert_eq!(teardown.persisted, vec!["sqs"]);
        teardown.run().await;
        // S3 writes no snapshot: its buckets are deleted from the store.
        assert!(state.reset_service("s3").unwrap().persisted.is_empty());

        let counts = counts.lock().clone();
        assert_eq!(counts.get("eventbridge"), Some(&1));
        assert_eq!(counts.get("apigateway"), Some(&1));
        assert_eq!(counts.get("apigatewayv2"), Some(&1));
        assert_eq!(counts.get("sqs"), Some(&1));
        assert_eq!(counts.len(), 4, "{counts:?}");
    }

    #[tokio::test]
    async fn full_reset_clears_registered_services_and_persists_everything() {
        let state = test_state();
        let swf = swf_state();
        add_domain(&swf, "123456789012");
        add_domain(&swf, "222222222222");
        let (late, counts) = counting_late(swf.clone());
        let expected = late.hooks.len();
        assert!(state.late.set(late).is_ok());

        let (_, teardown) = state.reset();
        teardown.run().await;
        assert_eq!(domains(&swf, "123456789012"), 0);
        assert_eq!(domains(&swf, "222222222222"), 0);
        let counts = counts.lock().clone();
        assert_eq!(counts.len(), expected);
        assert!(counts.values().all(|n| *n == 1), "{counts:?}");
    }

    #[tokio::test]
    async fn registered_service_resets_per_service_and_per_account() {
        let state = test_state();
        let swf = swf_state();
        let (late, counts) = counting_late(swf.clone());
        assert!(state.late.set(late).is_ok());

        add_domain(&swf, "123456789012");
        add_domain(&swf, "222222222222");
        let teardown = state
            .reset_service_for_account("swf", "222222222222")
            .unwrap();
        assert_eq!(teardown.persisted, vec!["swf"]);
        teardown.run().await;
        assert_eq!(domains(&swf, "123456789012"), 1);
        assert_eq!(domains(&swf, "222222222222"), 0);

        state.reset_service("swf").unwrap().run().await;
        assert_eq!(domains(&swf, "123456789012"), 0);
        assert_eq!(counts.lock().get("swf"), Some(&2));
    }

    #[test]
    fn unknown_services_and_account_resets_without_accounts_are_refused() {
        let state = test_state();
        let err = state.reset_service("nope").err().unwrap();
        assert_eq!(err, ResetError::UnknownService("nope".into()));
        assert_eq!(err.status(), axum::http::StatusCode::NOT_FOUND);
        let err = state.reset_service_for_account("nope", "1").err().unwrap();
        assert_eq!(err.status(), axum::http::StatusCode::NOT_FOUND);
        let err = state
            .reset_service_for_account("organizations", "123456789012")
            .err()
            .unwrap();
        assert_eq!(err, ResetError::NoAccountReset("organizations".into()));
        assert_eq!(err.status(), axum::http::StatusCode::BAD_REQUEST);
        assert!(err.message().contains("/_fakecloud/reset/organizations"));
    }

    #[test]
    fn per_account_ec2_reset_clears_only_that_account() {
        let state = test_state();
        for account in ["123456789012", "222222222222"] {
            state
                .ec2
                .write()
                .regional_mut(account, "us-east-1")
                .tags
                .insert("i-1".into(), Vec::new());
        }
        let mut teardown = Teardown::default();
        let entry = EXPLICIT.iter().find(|e| e.names.contains(&"ec2")).unwrap();
        (entry.reset_account.unwrap())(&state, "222222222222", &mut teardown);
        let ec2 = state.ec2.read();
        assert!(ec2
            .regional("222222222222", "us-east-1")
            .map_or(true, |s| !s.tags.contains_key("i-1")));
        assert!(ec2
            .regional("123456789012", "us-east-1")
            .unwrap()
            .tags
            .contains_key("i-1"));
    }

    #[test]
    fn every_name_resolves_to_one_service() {
        let services = reset_services(ResetServiceStates::empty());
        let mut seen = BTreeSet::new();
        for name in EXPLICIT
            .iter()
            .flat_map(|e| e.names.iter())
            .chain(services.iter().flat_map(|s| s.names.iter()))
        {
            assert!(seen.insert(*name), "`{name}` names two services");
        }
        // Every row and entry writes at least one snapshot, except S3.
        for e in EXPLICIT {
            assert!(!e.hooks.is_empty() || e.names == ["s3"], "{:?}", e.names);
        }
    }

    /// The wiring guarantee, checked against the server's own source: every
    /// snapshot store `main.rs` opens has a persist hook, every hook is
    /// written by some reset, and every hook a reset writes is registered.
    #[test]
    fn every_snapshot_store_is_reset_and_persisted() {
        let main =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs")).unwrap();
        // Every snapshot store goes through `snapshot_store_path`, which is
        // what lets the runtime check see it too.
        assert!(
            !main.contains("\"snapshot.json\""),
            "main.rs builds a snapshot path by hand; use reset::snapshot_store_path"
        );
        let quoted_after = |marker: &str| -> Vec<String> {
            main.match_indices(marker)
                .filter_map(|(i, _)| {
                    let rest = main[i + marker.len()..].trim_start();
                    let rest = rest.strip_prefix('"')?;
                    Some(rest[..rest.find('"')?].to_string())
                })
                .collect()
        };
        let stores = quoted_after("snapshot_store_path(&data_path,");
        assert!(stores.len() > 90, "found only {} stores", stores.len());
        let mut hooks: Vec<String> = Vec::new();
        for map in ["cfn_snapshot_hooks", "reset_only_hooks", "reset_hooks"] {
            hooks.extend(quoted_after(&format!("{map}.insert(")));
        }
        let hooks: BTreeSet<&str> = hooks.iter().map(String::as_str).collect();
        let services = reset_services(ResetServiceStates::empty());
        let reset = reset_hook_keys(&services);
        let gaps = wiring_gaps(stores.iter().map(String::as_str), &hooks, &reset);
        assert!(gaps.is_empty(), "{gaps:#?}");
    }

    #[test]
    fn snapshot_file_path_does_not_record_a_store() {
        let dir = "reset-test-unrecorded-store";
        let path = snapshot_file_path(Path::new("/data"), dir);
        assert_eq!(path, Path::new("/data").join(dir).join("snapshot.json"));
        assert!(!SNAPSHOT_STORES.lock().contains(dir));
        assert_eq!(snapshot_store_path(Path::new("/data"), dir), path);
        assert!(SNAPSHOT_STORES.lock().remove(dir));
    }

    #[test]
    fn wiring_gaps_names_each_kind_of_gap() {
        let hooks = BTreeSet::from(["sqs", "orphan-hook"]);
        let reset = BTreeSet::from(["sqs", "unhooked"]);
        let gaps = wiring_gaps(["sqs", "lonely"], &hooks, &reset);
        assert_eq!(
            gaps,
            vec![
                "snapshot store `lonely` has no persist hook `lonely`".to_string(),
                "snapshot store `lonely` is not cleared by any reset".to_string(),
                "persist hook `orphan-hook` is not written by any reset".to_string(),
                "reset writes persist hook `unhooked`, which is not registered".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn reset_deletes_s3_buckets_before_returning() {
        use fakecloud_persistence::S3Store;
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(fakecloud_persistence::s3::DiskS3Store::new(
            tmp.path().to_path_buf(),
            Arc::new(fakecloud_persistence::cache::BodyCache::new(0)),
        ));
        let meta = fakecloud_persistence::s3::BucketMeta {
            name: "b".into(),
            ..Default::default()
        };
        store.put_bucket_meta("kept", &meta).unwrap();
        store.put_bucket_meta("gone", &meta).unwrap();
        let state = test_state();
        {
            let mut s3 = state.s3.write();
            for (account, bucket) in [("123456789012", "kept"), ("222222222222", "gone")] {
                s3.get_or_create(account).buckets.insert(
                    bucket.to_string(),
                    fakecloud_s3::S3Bucket::new(bucket, "us-east-1", account),
                );
            }
        }
        let late = LateReset {
            s3_store: Some(store.clone()),
            ..Default::default()
        };
        assert!(state.late.set(late).is_ok());

        let teardown = state
            .reset_service_for_account("s3", "222222222222")
            .unwrap();
        // Deleted by the time the reset returns, not by a deferred task...
        assert!(store.bucket_state_exists("kept"));
        assert!(!store.bucket_state_exists("gone"));
        // ...so a bucket recreated under the reset name right after the reset
        // keeps its directory when the teardown runs.
        store.put_bucket_meta("gone", &meta).unwrap();
        teardown.run().await;
        assert!(store.bucket_state_exists("gone"));

        let (_, teardown) = state.reset();
        assert!(!store.bucket_state_exists("kept"));
        teardown.run().await;
        assert!(state.s3.read().default_ref().buckets.is_empty());
    }

    /// A full reset recreates the default S3 account; it must keep the write
    /// side effects the S3 service installed, or objects other services
    /// deliver after a reset would silently stop firing notifications and
    /// replicating.
    #[tokio::test]
    async fn full_s3_reset_keeps_the_installed_write_side_effects() {
        let state = test_state();
        let _svc = fakecloud_s3::S3Service::new(
            state.s3.clone(),
            Arc::new(fakecloud_core::delivery::DeliveryBus::new()),
        );
        assert!(state.s3.read().default_ref().side_effects.is_some());
        let (_, teardown) = state.reset();
        teardown.run().await;
        assert!(state.s3.read().default_ref().side_effects.is_some());
    }
}
