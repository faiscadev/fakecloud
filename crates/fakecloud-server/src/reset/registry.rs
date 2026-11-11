//! Snapshot-backed services the reset endpoints clear through a registered
//! entry rather than a dedicated [`super::ResetState`] field.

use std::sync::Arc;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

use super::Teardown;

/// A service the reset endpoints clear through a registered entry.
pub(crate) struct ServiceReset {
    /// The names `/_fakecloud/reset/{service}` accepts for it.
    pub(super) names: &'static [&'static str],
    /// Its persist-hook key.
    pub(super) hook: &'static str,
    pub(super) reset_all: Box<ResetAllFn>,
    pub(super) reset_account: Box<ResetAccountFn>,
}

/// Clears a service in every account, queuing any teardown it needs.
type ResetAllFn = dyn Fn(&mut Teardown) + Send + Sync;
/// Clears one account of a service, queuing any teardown it needs.
type ResetAccountFn = dyn Fn(&str, &mut Teardown) + Send + Sync;

impl ServiceReset {
    pub(crate) fn new(
        names: &'static [&'static str],
        hook: &'static str,
        reset_all: impl Fn(&mut Teardown) + Send + Sync + 'static,
        reset_account: impl Fn(&str, &mut Teardown) + Send + Sync + 'static,
    ) -> Self {
        Self {
            names,
            hook,
            reset_all: Box::new(reset_all),
            reset_account: Box::new(reset_account),
        }
    }

    /// A service whose whole state is one [`MultiAccountState`].
    pub(crate) fn multi_account<T>(
        names: &'static [&'static str],
        hook: &'static str,
        state: Arc<parking_lot::RwLock<MultiAccountState<T>>>,
    ) -> Self
    where
        T: AccountState + Send + Sync + 'static,
    {
        let all = state.clone();
        Self::new(
            names,
            hook,
            move |_| all.write().reset(),
            move |account_id, _| state.write().reset_account(account_id),
        )
    }
}

/// The shared state of every snapshot-backed service without a dedicated
/// [`ResetState`] field, plus the runtimes their reset tears down.
pub(crate) struct ResetServiceStates {
    pub account: fakecloud_account::SharedAccountState,
    pub amplify: fakecloud_amplify::SharedAmplifyState,
    pub appconfig: fakecloud_appconfig::SharedAppConfigState,
    pub appsync: fakecloud_appsync::SharedAppSyncState,
    pub autoscaling: fakecloud_autoscaling::SharedAutoScalingState,
    pub backup: fakecloud_backup::SharedBackupState,
    pub batch: fakecloud_batch::SharedBatchState,
    pub ce: fakecloud_ce::SharedCeState,
    pub cloudcontrol: fakecloud_cloudcontrol::SharedCloudControlState,
    pub cloudtrail: fakecloud_cloudtrail::SharedCloudTrailState,
    pub codeartifact: fakecloud_codeartifact::SharedCodeArtifactState,
    pub codebuild: fakecloud_codebuild::SharedCodeBuildState,
    pub codecommit: fakecloud_codecommit::SharedCodeCommitState,
    pub codeconnections: fakecloud_codeconnections::SharedCodeConnectionsState,
    pub codedeploy: fakecloud_codedeploy::SharedCodeDeployState,
    pub codepipeline: fakecloud_codepipeline::SharedCodePipelineState,
    pub comprehend: fakecloud_comprehend::SharedComprehendState,
    pub dms: fakecloud_dms::SharedDmsState,
    pub docdb: fakecloud_docdb::SharedDocDbState,
    pub dsql: fakecloud_dsql::SharedDsqlState,
    pub efs: fakecloud_efs::SharedEfsState,
    pub eks: fakecloud_eks::SharedEksState,
    pub elasticbeanstalk: fakecloud_elasticbeanstalk::SharedEbState,
    pub elbv2: fakecloud_elbv2::SharedElbv2State,
    pub emr: fakecloud_emr::SharedEmrState,
    pub fis: fakecloud_fis::SharedFisState,
    pub glacier: fakecloud_glacier::SharedGlacierState,
    pub identitystore: fakecloud_identitystore::SharedIdentityStoreState,
    pub iot: fakecloud_iot::SharedIotState,
    pub iotdata: fakecloud_iotdata::SharedIotDataState,
    pub iotwireless: fakecloud_iotwireless::SharedIotWirelessState,
    pub kafka: fakecloud_kafka::SharedKafkaState,
    pub kinesisanalyticsv2: fakecloud_kinesisanalyticsv2::SharedKa2State,
    pub lakeformation: fakecloud_lakeformation::SharedLakeFormationState,
    pub managedblockchain: fakecloud_managedblockchain::SharedManagedBlockchainState,
    pub mediaconvert: fakecloud_mediaconvert::SharedMediaConvertState,
    pub memorydb: fakecloud_memorydb::SharedMemoryDbState,
    pub mq: fakecloud_mq::SharedMqState,
    pub mwaa: fakecloud_mwaa::SharedMwaaState,
    pub neptune: fakecloud_neptune::SharedNeptuneState,
    pub opensearch: fakecloud_opensearch::SharedOpenSearchState,
    pub pinpoint: fakecloud_pinpoint::SharedPinpointState,
    pub pipes: fakecloud_pipes::SharedPipesState,
    pub ram: fakecloud_ram::SharedRamState,
    pub redshift: fakecloud_redshift::SharedRedshiftState,
    pub resource_groups: fakecloud_resource_groups::SharedResourceGroupsState,
    pub resource_groups_tagging:
        fakecloud_resource_groups_tagging::SharedResourceGroupsTaggingState,
    pub s3tables: fakecloud_s3tables::SharedS3TablesState,
    pub sagemaker: fakecloud_sagemaker::SharedSageMakerState,
    pub serverlessrepo: fakecloud_serverlessrepo::SharedServerlessRepoState,
    pub servicediscovery: fakecloud_servicediscovery::SharedServiceDiscoveryState,
    pub shield: fakecloud_shield::SharedShieldState,
    pub ssoadmin: fakecloud_ssoadmin::SharedSsoAdminState,
    pub support: fakecloud_support::SharedSupportState,
    pub swf: fakecloud_swf::SharedSwfState,
    pub textract: fakecloud_textract::SharedTextractState,
    pub timestream: fakecloud_timestream::SharedTimestreamState,
    pub transcribe: fakecloud_transcribe::SharedTranscribeState,
    pub transfer: fakecloud_transfer::SharedTransferState,
    pub translate: fakecloud_translate::SharedTranslateState,
    pub verifiedpermissions: fakecloud_verifiedpermissions::SharedVerifiedPermissionsState,
    pub xray: fakecloud_xray::SharedXrayState,
    pub kafka_runtime: Option<Arc<fakecloud_kafka::KafkaRuntime>>,
    pub flink_runtime: Option<Arc<fakecloud_kinesisanalyticsv2::FlinkRuntime>>,
    pub mq_runtime: Option<Arc<fakecloud_mq::MqRuntime>>,
    pub opensearch_runtime: Option<Arc<fakecloud_opensearch::OpenSearchRuntime>>,
    pub codebuild_running: Option<fakecloud_codebuild::runtime::RunningBuilds>,
}

/// Accounts-map services (a `BTreeMap` of account id to state): a full reset
/// drops every account, an account reset that account.
macro_rules! accounts_map_reset {
    ($names:expr, $hook:expr, $state:expr, $ty:ty) => {{
        let all = $state.clone();
        let one = $state;
        ServiceReset::new(
            $names,
            $hook,
            move |_| *all.write() = <$ty>::default(),
            move |account_id, _| {
                one.write().accounts.remove(account_id);
            },
        )
    }};
}

/// The [`ServiceReset`] entries of the services [`ResetServiceStates`] holds.
pub(crate) fn reset_services(s: ResetServiceStates) -> Vec<ServiceReset> {
    let mut services = vec![
        ServiceReset::multi_account(&["account"], "account", s.account),
        ServiceReset::multi_account(&["amplify"], "amplify", s.amplify),
        ServiceReset::multi_account(&["appconfig"], "appconfig", s.appconfig),
        ServiceReset::multi_account(&["appsync"], "appsync", s.appsync),
        ServiceReset::multi_account(&["backup"], "backup", s.backup),
        ServiceReset::multi_account(&["ce"], "ce", s.ce),
        ServiceReset::multi_account(
            &["cloudcontrol", "cloudcontrolapi"],
            "cloudcontrol",
            s.cloudcontrol,
        ),
        ServiceReset::multi_account(&["cloudtrail"], "cloudtrail", s.cloudtrail),
        ServiceReset::multi_account(&["codeartifact"], "codeartifact", s.codeartifact),
        ServiceReset::multi_account(&["codecommit"], "codecommit", s.codecommit),
        ServiceReset::multi_account(
            &["codeconnections", "codestar-connections"],
            "codeconnections",
            s.codeconnections,
        ),
        ServiceReset::multi_account(&["codedeploy"], "codedeploy", s.codedeploy),
        ServiceReset::multi_account(&["codepipeline"], "codepipeline", s.codepipeline),
        ServiceReset::multi_account(&["comprehend"], "comprehend", s.comprehend),
        ServiceReset::multi_account(&["dms"], "dms", s.dms),
        ServiceReset::multi_account(&["docdb"], "docdb", s.docdb),
        ServiceReset::multi_account(&["dsql"], "dsql", s.dsql),
        ServiceReset::multi_account(&["elasticfilesystem", "efs"], "elasticfilesystem", s.efs),
        ServiceReset::multi_account(&["eks"], "eks", s.eks),
        ServiceReset::multi_account(&["emr", "elasticmapreduce"], "emr", s.emr),
        ServiceReset::multi_account(&["fis"], "fis", s.fis),
        ServiceReset::multi_account(&["glacier"], "glacier", s.glacier),
        ServiceReset::multi_account(&["identitystore"], "identitystore", s.identitystore),
        ServiceReset::multi_account(&["iot"], "iot", s.iot),
        ServiceReset::multi_account(&["iotdata", "iot-data"], "iotdata", s.iotdata),
        ServiceReset::multi_account(&["iotwireless"], "iotwireless", s.iotwireless),
        ServiceReset::multi_account(&["lakeformation"], "lakeformation", s.lakeformation),
        ServiceReset::multi_account(
            &["managedblockchain"],
            "managedblockchain",
            s.managedblockchain,
        ),
        ServiceReset::multi_account(&["mediaconvert"], "mediaconvert", s.mediaconvert),
        ServiceReset::multi_account(&["memorydb"], "memorydb", s.memorydb),
        ServiceReset::multi_account(&["mwaa", "airflow"], "mwaa", s.mwaa),
        ServiceReset::multi_account(&["neptune"], "neptune", s.neptune),
        ServiceReset::multi_account(&["pinpoint", "mobiletargeting"], "pinpoint", s.pinpoint),
        ServiceReset::multi_account(&["ram"], "ram", s.ram),
        ServiceReset::multi_account(&["resource-groups"], "resource-groups", s.resource_groups),
        ServiceReset::multi_account(
            &["resource-groups-tagging", "tagging"],
            "resource-groups-tagging",
            s.resource_groups_tagging,
        ),
        ServiceReset::multi_account(&["s3tables"], "s3tables", s.s3tables),
        ServiceReset::multi_account(&["sagemaker"], "sagemaker", s.sagemaker),
        ServiceReset::multi_account(&["serverlessrepo"], "serverlessrepo", s.serverlessrepo),
        ServiceReset::multi_account(
            &["servicediscovery"],
            "servicediscovery",
            s.servicediscovery,
        ),
        ServiceReset::multi_account(&["shield"], "shield", s.shield),
        ServiceReset::multi_account(&["ssoadmin", "sso"], "ssoadmin", s.ssoadmin),
        ServiceReset::multi_account(&["support"], "support", s.support),
        ServiceReset::multi_account(&["swf"], "swf", s.swf),
        ServiceReset::multi_account(&["textract"], "textract", s.textract),
        ServiceReset::multi_account(&["timestream"], "timestream", s.timestream),
        ServiceReset::multi_account(&["transcribe"], "transcribe", s.transcribe),
        ServiceReset::multi_account(&["transfer"], "transfer", s.transfer),
        ServiceReset::multi_account(&["translate"], "translate", s.translate),
        ServiceReset::multi_account(
            &["verifiedpermissions"],
            "verifiedpermissions",
            s.verifiedpermissions,
        ),
        ServiceReset::multi_account(&["xray"], "xray", s.xray),
        accounts_map_reset!(
            &["autoscaling"],
            "autoscaling",
            s.autoscaling,
            fakecloud_autoscaling::AutoScalingAccounts
        ),
        accounts_map_reset!(&["batch"], "batch", s.batch, fakecloud_batch::BatchAccounts),
        accounts_map_reset!(
            &["elasticbeanstalk"],
            "elasticbeanstalk",
            s.elasticbeanstalk,
            fakecloud_elasticbeanstalk::EbAccounts
        ),
        accounts_map_reset!(&["pipes"], "pipes", s.pipes, fakecloud_pipes::PipesAccounts),
        accounts_map_reset!(
            &["redshift"],
            "redshift",
            s.redshift,
            fakecloud_redshift::RedshiftAccounts
        ),
    ];
    services.push({
        let all = s.elbv2.clone();
        let one = s.elbv2;
        // The load balancers' listeners close on the data plane's next
        // reconcile, once their rows are gone.
        ServiceReset::new(
            &["elbv2", "elasticloadbalancing"],
            "elbv2",
            move |_| *all.write() = fakecloud_elbv2::Elbv2Accounts::new(),
            move |account_id, _| one.write().remove_account(account_id),
        )
    });
    services.push(codebuild_reset(s.codebuild, s.codebuild_running));
    services.push(mq_reset(s.mq, s.mq_runtime));
    services.push(opensearch_reset(s.opensearch, s.opensearch_runtime));
    services.push(kafka_reset(s.kafka, s.kafka_runtime));
    services.push(flink_reset(s.kinesisanalyticsv2, s.flink_runtime));
    services
}

/// The rows' ids plus the ids a runtime tracked, both collected under the
/// state lock right before the reset: exactly the backing containers the
/// reset ends. A container a resource created after the reset starts is in
/// neither, so it is never reached.
fn reset_targets(rows: Vec<String>, tracked: Option<Vec<String>>) -> Vec<String> {
    let mut targets: std::collections::BTreeSet<String> = rows.into_iter().collect();
    targets.extend(tracked.unwrap_or_default());
    targets.into_iter().collect()
}

/// The tracked ARNs that belong to `account`.
fn tracked_in_account(tracked: Option<Vec<String>>, account: &str) -> Option<Vec<String>> {
    tracked.map(|arns| {
        arns.into_iter()
            .filter(|arn| fakecloud_aws::arn::account_of(arn) == Some(account))
            .collect()
    })
}

type RunningBuilds = fakecloud_codebuild::runtime::RunningBuilds;

/// The build and build-batch ids of an account's CodeBuild state.
fn codebuild_builds(state: &fakecloud_codebuild::CodeBuildState) -> Vec<String> {
    state
        .builds
        .keys()
        .chain(state.build_batches.keys())
        .cloned()
        .collect()
}

/// Take the containers of `ids` out of tracking now (under the state lock)
/// and kill them as part of the teardown.
fn kill_builds(teardown: &mut Teardown, running: &Option<RunningBuilds>, ids: Vec<String>) {
    if let Some(running) = running.clone() {
        let containers = running.take(ids);
        if !containers.is_empty() {
            teardown.push(async move { running.kill(containers).await });
        }
    }
}

/// CodeBuild: a running build's container goes with its record. Without
/// this the build would only notice at its next phase boundary.
fn codebuild_reset(
    state: fakecloud_codebuild::SharedCodeBuildState,
    running: Option<RunningBuilds>,
) -> ServiceReset {
    let all = state.clone();
    let all_running = running.clone();
    ServiceReset::new(
        &["codebuild"],
        "codebuild",
        move |teardown| {
            let mut mas = all.write();
            let ids = mas.iter().flat_map(|(_, s)| codebuild_builds(s)).collect();
            mas.reset();
            kill_builds(teardown, &all_running, ids);
        },
        move |account_id, teardown| {
            let mut mas = state.write();
            let ids = mas
                .get(account_id)
                .map(codebuild_builds)
                .unwrap_or_default();
            mas.reset_account(account_id);
            kill_builds(teardown, &running, ids);
        },
    )
}

/// Stop the MQ brokers `brokers` as part of the teardown.
fn stop_brokers(
    teardown: &mut Teardown,
    runtime: &Option<Arc<fakecloud_mq::MqRuntime>>,
    brokers: Vec<String>,
) {
    if let Some(rt) = runtime.clone() {
        teardown.push(async move {
            for broker in brokers {
                rt.stop_broker(&broker).await;
            }
        });
    }
}

/// Amazon MQ: the brokers' backing containers go with their rows.
fn mq_reset(
    state: fakecloud_mq::SharedMqState,
    runtime: Option<Arc<fakecloud_mq::MqRuntime>>,
) -> ServiceReset {
    let all = state.clone();
    let all_rt = runtime.clone();
    ServiceReset::new(
        &["mq"],
        "mq",
        move |teardown| {
            let brokers = {
                let mut mas = all.write();
                let rows = mas
                    .iter()
                    .flat_map(|(_, d)| d.brokers.keys().cloned())
                    .collect();
                let tracked = all_rt.as_ref().map(|rt| rt.tracked_brokers());
                mas.reset();
                reset_targets(rows, tracked)
            };
            stop_brokers(teardown, &all_rt, brokers);
        },
        // Unlike the full reset, an account reset stops only the brokers of
        // the account's rows: the runtime tracks containers by broker id,
        // which does not name an account, so a tracked container whose row is
        // already gone cannot be attributed to one.
        move |account_id, teardown| {
            let brokers = {
                let mut mas = state.write();
                let rows = mas
                    .get(account_id)
                    .map(|d| d.brokers.keys().cloned().collect())
                    .unwrap_or_default();
                mas.reset_account(account_id);
                rows
            };
            stop_brokers(teardown, &runtime, brokers);
        },
    )
}

/// `(domain ARN, data volume)` of every domain in an OpenSearch state.
fn opensearch_domains(
    state: &fakecloud_opensearch::OpenSearchState,
) -> Vec<(String, Option<String>)> {
    state
        .domains
        .values()
        .map(|d| {
            (
                d.arn.clone(),
                fakecloud_opensearch::backing::domain_spec(d).map(|s| s.volume),
            )
        })
        .collect()
}

/// Remove the backing clusters (containers and data volumes) of `domains`
/// as part of the teardown.
fn destroy_domains(
    teardown: &mut Teardown,
    runtime: &Option<Arc<fakecloud_opensearch::OpenSearchRuntime>>,
    domains: Vec<(String, Option<String>)>,
) {
    if let Some(rt) = runtime.clone() {
        teardown.push(async move {
            for (arn, volume) in domains {
                rt.destroy_domain(&arn, volume.as_deref()).await;
            }
        });
    }
}

/// OpenSearch / Elasticsearch: the domains' backing clusters go with their
/// rows. A tracked container whose row is already gone is still removed by a
/// full reset; an account reset reaches the tracked ones whose ARN names the
/// account.
fn opensearch_reset(
    state: fakecloud_opensearch::SharedOpenSearchState,
    runtime: Option<Arc<fakecloud_opensearch::OpenSearchRuntime>>,
) -> ServiceReset {
    let all = state.clone();
    let all_rt = runtime.clone();
    ServiceReset::new(
        &["es", "opensearch"],
        "es",
        move |teardown| {
            let domains = {
                let mut mas = all.write();
                let mut rows: Vec<(String, Option<String>)> = mas
                    .iter()
                    .flat_map(|(_, st)| opensearch_domains(st))
                    .collect();
                let known: std::collections::BTreeSet<String> =
                    rows.iter().map(|(arn, _)| arn.clone()).collect();
                if let Some(rt) = &all_rt {
                    rows.extend(
                        rt.tracked_domains()
                            .into_iter()
                            .filter(|arn| !known.contains(arn))
                            .map(|arn| (arn, None)),
                    );
                }
                mas.reset();
                rows
            };
            destroy_domains(teardown, &all_rt, domains);
        },
        move |account_id, teardown| {
            let domains = {
                let mut mas = state.write();
                let mut rows: Vec<(String, Option<String>)> = mas
                    .get(account_id)
                    .map(opensearch_domains)
                    .unwrap_or_default();
                let known: std::collections::BTreeSet<String> =
                    rows.iter().map(|(arn, _)| arn.clone()).collect();
                let tracked =
                    tracked_in_account(runtime.as_ref().map(|rt| rt.tracked_domains()), account_id);
                rows.extend(
                    tracked
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|arn| !known.contains(arn))
                        .map(|arn| (arn, None)),
                );
                mas.reset_account(account_id);
                rows
            };
            destroy_domains(teardown, &runtime, domains);
        },
    )
}

/// Stop the MSK clusters `clusters` as part of the teardown.
fn stop_clusters(
    teardown: &mut Teardown,
    runtime: &Option<Arc<fakecloud_kafka::KafkaRuntime>>,
    clusters: Vec<String>,
) {
    if let Some(rt) = runtime.clone() {
        teardown.push(async move {
            for cluster in clusters {
                rt.stop_broker(&cluster).await;
            }
        });
    }
}

/// Amazon MSK: the clusters' backing broker containers go with their rows.
fn kafka_reset(
    state: fakecloud_kafka::SharedKafkaState,
    runtime: Option<Arc<fakecloud_kafka::KafkaRuntime>>,
) -> ServiceReset {
    let all = state.clone();
    let all_rt = runtime.clone();
    ServiceReset::new(
        &["kafka", "msk"],
        "kafka",
        move |teardown| {
            let clusters = {
                let mut mas = all.write();
                let rows = mas
                    .iter()
                    .flat_map(|(_, d)| d.clusters.keys().cloned())
                    .collect();
                let tracked = all_rt.as_ref().map(|rt| rt.tracked_clusters());
                mas.reset();
                reset_targets(rows, tracked)
            };
            stop_clusters(teardown, &all_rt, clusters);
        },
        move |account_id, teardown| {
            let clusters = {
                let mut mas = state.write();
                let rows = mas
                    .get(account_id)
                    .map(|d| d.clusters.keys().cloned().collect())
                    .unwrap_or_default();
                // Tracked by cluster ARN, which names the account.
                let tracked = tracked_in_account(
                    runtime.as_ref().map(|rt| rt.tracked_clusters()),
                    account_id,
                );
                mas.reset_account(account_id);
                reset_targets(rows, tracked)
            };
            stop_clusters(teardown, &runtime, clusters);
        },
    )
}

/// `(application ARN, backing container)` of every Flink application in an
/// account's state.
fn flink_apps(state: &fakecloud_kinesisanalyticsv2::Ka2State) -> Vec<(String, Option<String>)> {
    state
        .applications
        .values()
        .map(|app| {
            (
                app.arn.clone(),
                app.flink_binding.as_ref().map(|b| b.container_id.clone()),
            )
        })
        .collect()
}

/// The applications' rows plus the clusters the runtime still tracks for
/// applications whose rows are already gone (a delete whose teardown has not
/// run yet).
fn flink_targets(
    mut apps: Vec<(String, Option<String>)>,
    tracked: Option<Vec<String>>,
) -> Vec<(String, Option<String>)> {
    for arn in tracked.unwrap_or_default() {
        if !apps.iter().any(|(a, _)| *a == arn) {
            apps.push((arn, None));
        }
    }
    apps
}

type FlinkRuntime = fakecloud_kinesisanalyticsv2::FlinkRuntime;

/// Remove the Flink clusters of `apps` as part of the teardown.
fn remove_clusters(
    teardown: &mut Teardown,
    runtime: &Option<Arc<FlinkRuntime>>,
    apps: Vec<(String, Option<String>)>,
) {
    if let Some(rt) = runtime.clone() {
        teardown.push(async move {
            for (arn, container) in apps {
                rt.remove_cluster(&arn, container.as_deref()).await;
            }
        });
    }
}

/// Managed Service for Apache Flink: the applications' backing Flink
/// clusters go with their rows.
fn flink_reset(
    state: fakecloud_kinesisanalyticsv2::SharedKa2State,
    runtime: Option<Arc<FlinkRuntime>>,
) -> ServiceReset {
    let all = state.clone();
    let all_rt = runtime.clone();
    ServiceReset::new(
        &["kinesisanalyticsv2", "kinesisanalytics"],
        "kinesisanalyticsv2",
        move |teardown| {
            let apps = {
                let mut mas = all.write();
                let rows = mas.iter().flat_map(|(_, s)| flink_apps(s)).collect();
                let tracked = all_rt.as_ref().map(|rt| rt.tracked_apps());
                mas.reset();
                flink_targets(rows, tracked)
            };
            remove_clusters(teardown, &all_rt, apps);
        },
        move |account_id, teardown| {
            let apps = {
                let mut mas = state.write();
                let rows = mas.get(account_id).map(flink_apps).unwrap_or_default();
                // Tracked by application ARN, which names the account.
                let tracked =
                    tracked_in_account(runtime.as_ref().map(|rt| rt.tracked_apps()), account_id);
                mas.reset_account(account_id);
                flink_targets(rows, tracked)
            };
            remove_clusters(teardown, &runtime, apps);
        },
    )
}

#[cfg(test)]
impl ResetServiceStates {
    /// Every state empty, no runtimes.
    pub(super) fn empty() -> Self {
        fn ma<T: AccountState>() -> Arc<parking_lot::RwLock<MultiAccountState<T>>> {
            Arc::new(parking_lot::RwLock::new(MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "",
            )))
        }
        Self {
            account: ma(),
            amplify: ma(),
            appconfig: ma(),
            appsync: ma(),
            autoscaling: Arc::default(),
            backup: ma(),
            batch: Arc::default(),
            ce: ma(),
            cloudcontrol: ma(),
            cloudtrail: ma(),
            codeartifact: ma(),
            codebuild: ma(),
            codecommit: ma(),
            codeconnections: ma(),
            codedeploy: ma(),
            codepipeline: ma(),
            comprehend: ma(),
            dms: ma(),
            docdb: ma(),
            dsql: ma(),
            efs: ma(),
            eks: ma(),
            elasticbeanstalk: Arc::default(),
            elbv2: Arc::default(),
            emr: ma(),
            fis: ma(),
            glacier: ma(),
            identitystore: ma(),
            iot: ma(),
            iotdata: ma(),
            iotwireless: ma(),
            kafka: ma(),
            kinesisanalyticsv2: ma(),
            lakeformation: ma(),
            managedblockchain: ma(),
            mediaconvert: ma(),
            memorydb: ma(),
            mq: ma(),
            mwaa: ma(),
            neptune: ma(),
            opensearch: ma(),
            pinpoint: ma(),
            pipes: Arc::default(),
            ram: ma(),
            redshift: Arc::default(),
            resource_groups: ma(),
            resource_groups_tagging: ma(),
            s3tables: ma(),
            sagemaker: ma(),
            serverlessrepo: ma(),
            servicediscovery: ma(),
            shield: ma(),
            ssoadmin: ma(),
            support: ma(),
            swf: ma(),
            textract: ma(),
            timestream: ma(),
            transcribe: ma(),
            transfer: ma(),
            translate: ma(),
            verifiedpermissions: ma(),
            xray: ma(),
            kafka_runtime: None,
            flink_runtime: None,
            mq_runtime: None,
            opensearch_runtime: None,
            codebuild_running: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_targets_are_rows_plus_tracked() {
        assert_eq!(
            reset_targets(
                vec!["b".into(), "a".into()],
                Some(vec!["c".into(), "a".into()])
            ),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        assert_eq!(reset_targets(vec!["a".into()], None), vec!["a".to_string()]);
    }

    #[test]
    fn tracked_in_account_keeps_that_accounts_arns() {
        let tracked = vec![
            "arn:aws:kafka:us-east-1:111111111111:cluster/a/1".to_string(),
            "arn:aws:kafka:us-east-1:222222222222:cluster/b/2".to_string(),
        ];
        assert_eq!(
            tracked_in_account(Some(tracked), "222222222222"),
            Some(vec![
                "arn:aws:kafka:us-east-1:222222222222:cluster/b/2".to_string()
            ])
        );
        assert_eq!(tracked_in_account(None, "222222222222"), None);
    }

    #[test]
    fn flink_targets_add_tracked_clusters_without_rows() {
        let apps = vec![("arn:a".to_string(), Some("c-a".to_string()))];
        assert_eq!(
            flink_targets(apps, Some(vec!["arn:a".into(), "arn:gone".into()])),
            vec![
                ("arn:a".to_string(), Some("c-a".to_string())),
                ("arn:gone".to_string(), None)
            ]
        );
    }

    #[test]
    fn mq_reset_clears_rows_per_account_and_everywhere() {
        let states = ResetServiceStates::empty();
        let mq = states.mq.clone();
        for account in ["123456789012", "222222222222"] {
            mq.write()
                .get_or_create(account)
                .brokers
                .insert("b-1".into(), serde_json::json!({}));
        }
        let entry = mq_reset(mq.clone(), None);
        let mut teardown = Teardown::default();
        (entry.reset_account)("222222222222", &mut teardown);
        assert!(mq.read().get("222222222222").unwrap().brokers.is_empty());
        assert_eq!(mq.read().get("123456789012").unwrap().brokers.len(), 1);
        (entry.reset_all)(&mut teardown);
        assert!(mq.read().default_ref().brokers.is_empty());
    }
}
