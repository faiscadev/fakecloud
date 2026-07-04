//! AWS AppConfig + AppConfig Data restJson1 service handler.
//!
//! One handler serves both AWS model-services (they share the SigV4 signing
//! name `appconfig`). Control-plane operations live under `/applications`,
//! `/deploymentstrategies`, `/extensions`, `/extensionassociations`,
//! `/experimentdefinitions`, `/settings` and `/tags`; the AppConfig Data plane
//! lives under `/configurationsessions` and `/configuration`. Requests are
//! routed by method + path segments.
//!
//! Deployments settle to `COMPLETE` synchronously so a Describe waiter (and
//! Terraform) observes the terminal state without a background engine.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::header::{HeaderName, HeaderValue};
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    application_arn, deployment_strategy_arn, environment_arn, extension_arn,
    extension_association_arn, profile_arn, ApplicationRecord, DeploymentRecord, EnvironmentRecord,
    ExperimentRecord, HostedVersionRecord, ProfileRecord, SessionRecord, SharedAppConfigState,
    TagMap,
};

pub const APPCONFIG_ACTIONS: &[&str] = &[
    // ---- appconfig control plane ----
    "CreateApplication",
    "CreateConfigurationProfile",
    "CreateDeploymentStrategy",
    "CreateEnvironment",
    "CreateExperimentDefinition",
    "CreateExtension",
    "CreateExtensionAssociation",
    "CreateHostedConfigurationVersion",
    "DeleteApplication",
    "DeleteConfigurationProfile",
    "DeleteDeploymentStrategy",
    "DeleteEnvironment",
    "DeleteExperimentDefinition",
    "DeleteExtension",
    "DeleteExtensionAssociation",
    "DeleteHostedConfigurationVersion",
    "GetAccountSettings",
    "GetApplication",
    "GetConfiguration",
    "GetConfigurationProfile",
    "GetDeployment",
    "GetDeploymentStrategy",
    "GetEnvironment",
    "GetExperimentDefinition",
    "GetExperimentRun",
    "GetExtension",
    "GetExtensionAssociation",
    "GetHostedConfigurationVersion",
    "ListApplications",
    "ListConfigurationProfiles",
    "ListDeploymentStrategies",
    "ListDeployments",
    "ListEnvironments",
    "ListExperimentDefinitions",
    "ListExperimentRunEvents",
    "ListExperimentRuns",
    "ListExtensionAssociations",
    "ListExtensions",
    "ListHostedConfigurationVersions",
    "ListTagsForResource",
    "StartDeployment",
    "StartExperimentRun",
    "StopDeployment",
    "StopExperimentRun",
    "TagResource",
    "UntagResource",
    "UpdateAccountSettings",
    "UpdateApplication",
    "UpdateConfigurationProfile",
    "UpdateDeploymentStrategy",
    "UpdateEnvironment",
    "UpdateExperimentDefinition",
    "UpdateExperimentRun",
    "UpdateExtension",
    "UpdateExtensionAssociation",
    "ValidateConfiguration",
    // ---- appconfigdata data plane ----
    "GetLatestConfiguration",
    "StartConfigurationSession",
];

pub struct AppConfigService {
    state: SharedAppConfigState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

/// The decoded path labels for a route, in URI order.
type Labels = Vec<String>;

impl AppConfigService {
    pub fn new(state: SharedAppConfigState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Route a request to `(action, path-labels)` by method + path segments.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Labels)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<&str> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed.split('/').collect()
        };
        let m = &req.method;
        let d = |s: &str| decode(s);
        let one = |a| Some((a, Vec::new()));
        macro_rules! l {
            ($a:expr, $($x:expr),*) => { Some(($a, vec![$($x),*])) };
        }
        let patch = Method::PATCH;
        match (m, segs.as_slice()) {
            // ---- applications ----
            (&Method::POST, ["applications"]) => one("CreateApplication"),
            (&Method::GET, ["applications"]) => one("ListApplications"),
            (&Method::GET, ["applications", app]) => l!("GetApplication", d(app)),
            (mth, ["applications", app]) if *mth == patch => l!("UpdateApplication", d(app)),
            (&Method::DELETE, ["applications", app]) => l!("DeleteApplication", d(app)),
            // ---- configuration profiles ----
            (&Method::POST, ["applications", app, "configurationprofiles"]) => {
                l!("CreateConfigurationProfile", d(app))
            }
            (&Method::GET, ["applications", app, "configurationprofiles"]) => {
                l!("ListConfigurationProfiles", d(app))
            }
            (&Method::GET, ["applications", app, "configurationprofiles", p]) => {
                l!("GetConfigurationProfile", d(app), d(p))
            }
            (mth, ["applications", app, "configurationprofiles", p]) if *mth == patch => {
                l!("UpdateConfigurationProfile", d(app), d(p))
            }
            (&Method::DELETE, ["applications", app, "configurationprofiles", p]) => {
                l!("DeleteConfigurationProfile", d(app), d(p))
            }
            (&Method::POST, ["applications", app, "configurationprofiles", p, "validators"]) => {
                l!("ValidateConfiguration", d(app), d(p))
            }
            // ---- hosted configuration versions ----
            (
                &Method::POST,
                ["applications", app, "configurationprofiles", p, "hostedconfigurationversions"],
            ) => l!("CreateHostedConfigurationVersion", d(app), d(p)),
            (
                &Method::GET,
                ["applications", app, "configurationprofiles", p, "hostedconfigurationversions"],
            ) => l!("ListHostedConfigurationVersions", d(app), d(p)),
            (
                &Method::GET,
                ["applications", app, "configurationprofiles", p, "hostedconfigurationversions", v],
            ) => l!("GetHostedConfigurationVersion", d(app), d(p), d(v)),
            (
                &Method::DELETE,
                ["applications", app, "configurationprofiles", p, "hostedconfigurationversions", v],
            ) => l!("DeleteHostedConfigurationVersion", d(app), d(p), d(v)),
            // ---- environments ----
            (&Method::POST, ["applications", app, "environments"]) => {
                l!("CreateEnvironment", d(app))
            }
            (&Method::GET, ["applications", app, "environments"]) => l!("ListEnvironments", d(app)),
            (&Method::GET, ["applications", app, "environments", e]) => {
                l!("GetEnvironment", d(app), d(e))
            }
            (mth, ["applications", app, "environments", e]) if *mth == patch => {
                l!("UpdateEnvironment", d(app), d(e))
            }
            (&Method::DELETE, ["applications", app, "environments", e]) => {
                l!("DeleteEnvironment", d(app), d(e))
            }
            // ---- deployments ----
            (&Method::POST, ["applications", app, "environments", e, "deployments"]) => {
                l!("StartDeployment", d(app), d(e))
            }
            (&Method::GET, ["applications", app, "environments", e, "deployments"]) => {
                l!("ListDeployments", d(app), d(e))
            }
            (&Method::GET, ["applications", app, "environments", e, "deployments", n]) => {
                l!("GetDeployment", d(app), d(e), d(n))
            }
            (&Method::DELETE, ["applications", app, "environments", e, "deployments", n]) => {
                l!("StopDeployment", d(app), d(e), d(n))
            }
            // ---- legacy GetConfiguration ----
            (&Method::GET, ["applications", app, "environments", e, "configurations", c]) => {
                l!("GetConfiguration", d(app), d(e), d(c))
            }
            // ---- deployment strategies ----
            (&Method::POST, ["deploymentstrategies"]) => one("CreateDeploymentStrategy"),
            (&Method::GET, ["deploymentstrategies"]) => one("ListDeploymentStrategies"),
            (&Method::GET, ["deploymentstrategies", id]) => l!("GetDeploymentStrategy", d(id)),
            (mth, ["deploymentstrategies", id]) if *mth == patch => {
                l!("UpdateDeploymentStrategy", d(id))
            }
            // NOTE: AWS's model spells the DELETE path `deployementstrategies`.
            (&Method::DELETE, ["deployementstrategies", id]) => {
                l!("DeleteDeploymentStrategy", d(id))
            }
            // ---- extensions ----
            (&Method::POST, ["extensions"]) => one("CreateExtension"),
            (&Method::GET, ["extensions"]) => one("ListExtensions"),
            (&Method::GET, ["extensions", id]) => l!("GetExtension", d(id)),
            (mth, ["extensions", id]) if *mth == patch => l!("UpdateExtension", d(id)),
            (&Method::DELETE, ["extensions", id]) => l!("DeleteExtension", d(id)),
            // ---- extension associations ----
            (&Method::POST, ["extensionassociations"]) => one("CreateExtensionAssociation"),
            (&Method::GET, ["extensionassociations"]) => one("ListExtensionAssociations"),
            (&Method::GET, ["extensionassociations", id]) => {
                l!("GetExtensionAssociation", d(id))
            }
            (mth, ["extensionassociations", id]) if *mth == patch => {
                l!("UpdateExtensionAssociation", d(id))
            }
            (&Method::DELETE, ["extensionassociations", id]) => {
                l!("DeleteExtensionAssociation", d(id))
            }
            // ---- experiment definitions ----
            (&Method::GET, ["experimentdefinitions"]) => one("ListExperimentDefinitions"),
            (&Method::POST, ["applications", app, "experimentdefinitions"]) => {
                l!("CreateExperimentDefinition", d(app))
            }
            (&Method::GET, ["applications", app, "experimentdefinitions", id]) => {
                l!("GetExperimentDefinition", d(app), d(id))
            }
            (mth, ["applications", app, "experimentdefinitions", id]) if *mth == patch => {
                l!("UpdateExperimentDefinition", d(app), d(id))
            }
            (&Method::DELETE, ["applications", app, "experimentdefinitions", id]) => {
                l!("DeleteExperimentDefinition", d(app), d(id))
            }
            // ---- experiment runs ----
            (
                &Method::POST,
                ["applications", app, "experimentdefinitions", id, "experimentruns"],
            ) => {
                l!("StartExperimentRun", d(app), d(id))
            }
            (
                &Method::GET,
                ["applications", app, "experimentdefinitions", id, "experimentruns"],
            ) => {
                l!("ListExperimentRuns", d(app), d(id))
            }
            (
                &Method::GET,
                ["applications", app, "experimentdefinitions", id, "experimentruns", run],
            ) => l!("GetExperimentRun", d(app), d(id), d(run)),
            (
                mth,
                ["applications", app, "experimentdefinitions", id, "experimentruns", run, "update"],
            ) if *mth == patch => l!("UpdateExperimentRun", d(app), d(id), d(run)),
            (
                mth,
                ["applications", app, "experimentdefinitions", id, "experimentruns", run, "stop"],
            ) if *mth == patch => l!("StopExperimentRun", d(app), d(id), d(run)),
            (
                &Method::GET,
                ["applications", app, "experimentdefinitions", id, "experimentruns", run, "events"],
            ) => l!("ListExperimentRunEvents", d(app), d(id), d(run)),
            // ---- account settings ----
            (&Method::GET, ["settings"]) => one("GetAccountSettings"),
            (mth, ["settings"]) if *mth == patch => one("UpdateAccountSettings"),
            // ---- tags ----
            (&Method::GET, ["tags", arn]) => l!("ListTagsForResource", d(arn)),
            (&Method::POST, ["tags", arn]) => l!("TagResource", d(arn)),
            (&Method::DELETE, ["tags", arn]) => l!("UntagResource", d(arn)),
            // ---- appconfigdata data plane ----
            (&Method::POST, ["configurationsessions"]) => one("StartConfigurationSession"),
            (&Method::GET, ["configuration"]) => one("GetLatestConfiguration"),
            _ => None,
        }
    }
}

const MUTATING: &[&str] = &[
    "CreateApplication",
    "CreateConfigurationProfile",
    "CreateDeploymentStrategy",
    "CreateEnvironment",
    "CreateExperimentDefinition",
    "CreateExtension",
    "CreateExtensionAssociation",
    "CreateHostedConfigurationVersion",
    "DeleteApplication",
    "DeleteConfigurationProfile",
    "DeleteDeploymentStrategy",
    "DeleteEnvironment",
    "DeleteExperimentDefinition",
    "DeleteExtension",
    "DeleteExtensionAssociation",
    "DeleteHostedConfigurationVersion",
    "StartDeployment",
    "StartExperimentRun",
    "StopDeployment",
    "StopExperimentRun",
    "TagResource",
    "UntagResource",
    "UpdateAccountSettings",
    "UpdateApplication",
    "UpdateConfigurationProfile",
    "UpdateDeploymentStrategy",
    "UpdateEnvironment",
    "UpdateExperimentDefinition",
    "UpdateExperimentRun",
    "UpdateExtension",
    "UpdateExtensionAssociation",
    "StartConfigurationSession",
];

#[async_trait]
impl AwsService for AppConfigService {
    fn service_name(&self) -> &str {
        "appconfig"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, labels) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let result = self.dispatch(action, &labels, &req);

        if MUTATING.contains(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        APPCONFIG_ACTIONS
    }
}

impl AppConfigService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &self,
        action: &str,
        l: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        crate::validation::validate(action, l, req)?;
        match action {
            // applications
            "CreateApplication" => self.create_application(req),
            "GetApplication" => self.get_application(req, &l[0]),
            "UpdateApplication" => self.update_application(req, &l[0]),
            "DeleteApplication" => self.delete_application(req, &l[0]),
            "ListApplications" => self.list_applications(req),
            // configuration profiles
            "CreateConfigurationProfile" => self.create_profile(req, &l[0]),
            "GetConfigurationProfile" => self.get_profile(req, &l[0], &l[1]),
            "UpdateConfigurationProfile" => self.update_profile(req, &l[0], &l[1]),
            "DeleteConfigurationProfile" => self.delete_profile(req, &l[0], &l[1]),
            "ListConfigurationProfiles" => self.list_profiles(req, &l[0]),
            "ValidateConfiguration" => self.validate_configuration(req, &l[0], &l[1]),
            // hosted configuration versions
            "CreateHostedConfigurationVersion" => self.create_hosted_version(req, &l[0], &l[1]),
            "GetHostedConfigurationVersion" => self.get_hosted_version(req, &l[0], &l[1], &l[2]),
            "DeleteHostedConfigurationVersion" => {
                self.delete_hosted_version(req, &l[0], &l[1], &l[2])
            }
            "ListHostedConfigurationVersions" => self.list_hosted_versions(req, &l[0], &l[1]),
            // environments
            "CreateEnvironment" => self.create_environment(req, &l[0]),
            "GetEnvironment" => self.get_environment(req, &l[0], &l[1]),
            "UpdateEnvironment" => self.update_environment(req, &l[0], &l[1]),
            "DeleteEnvironment" => self.delete_environment(req, &l[0], &l[1]),
            "ListEnvironments" => self.list_environments(req, &l[0]),
            // deployments
            "StartDeployment" => self.start_deployment(req, &l[0], &l[1]),
            "GetDeployment" => self.get_deployment(req, &l[0], &l[1], &l[2]),
            "StopDeployment" => self.stop_deployment(req, &l[0], &l[1], &l[2]),
            "ListDeployments" => self.list_deployments(req, &l[0], &l[1]),
            "GetConfiguration" => self.get_configuration(req, &l[0], &l[1], &l[2]),
            // deployment strategies
            "CreateDeploymentStrategy" => self.create_deployment_strategy(req),
            "GetDeploymentStrategy" => self.get_deployment_strategy(req, &l[0]),
            "UpdateDeploymentStrategy" => self.update_deployment_strategy(req, &l[0]),
            "DeleteDeploymentStrategy" => self.delete_deployment_strategy(req, &l[0]),
            "ListDeploymentStrategies" => self.list_deployment_strategies(req),
            // extensions
            "CreateExtension" => self.create_extension(req),
            "GetExtension" => self.get_extension(req, &l[0]),
            "UpdateExtension" => self.update_extension(req, &l[0]),
            "DeleteExtension" => self.delete_extension(req, &l[0]),
            "ListExtensions" => self.list_extensions(req),
            // extension associations
            "CreateExtensionAssociation" => self.create_extension_association(req),
            "GetExtensionAssociation" => self.get_extension_association(req, &l[0]),
            "UpdateExtensionAssociation" => self.update_extension_association(req, &l[0]),
            "DeleteExtensionAssociation" => self.delete_extension_association(req, &l[0]),
            "ListExtensionAssociations" => self.list_extension_associations(req),
            // experiments
            "CreateExperimentDefinition" => self.create_experiment_definition(req, &l[0]),
            "GetExperimentDefinition" => self.get_experiment_definition(req, &l[0], &l[1]),
            "UpdateExperimentDefinition" => self.update_experiment_definition(req, &l[0], &l[1]),
            "DeleteExperimentDefinition" => self.delete_experiment_definition(req, &l[0], &l[1]),
            "ListExperimentDefinitions" => self.list_experiment_definitions(req),
            "StartExperimentRun" => self.start_experiment_run(req, &l[0], &l[1]),
            "GetExperimentRun" => self.get_experiment_run(req, &l[0], &l[1], &l[2]),
            "UpdateExperimentRun" => self.update_experiment_run(req, &l[0], &l[1], &l[2]),
            "StopExperimentRun" => self.stop_experiment_run(req, &l[0], &l[1], &l[2]),
            "ListExperimentRuns" => self.list_experiment_runs(req, &l[0], &l[1]),
            "ListExperimentRunEvents" => self.list_experiment_run_events(req, &l[0], &l[1], &l[2]),
            // account settings
            "GetAccountSettings" => self.get_account_settings(req),
            "UpdateAccountSettings" => self.update_account_settings(req),
            // tags
            "ListTagsForResource" => self.list_tags(req, &l[0]),
            "TagResource" => self.tag_resource(req, &l[0]),
            "UntagResource" => self.untag_resource(req, &l[0]),
            // appconfigdata
            "StartConfigurationSession" => self.start_configuration_session(req),
            "GetLatestConfiguration" => self.get_latest_configuration(req),
            _ => Err(AwsServiceError::action_not_implemented("appconfig", action)),
        }
    }
}

include!("handlers.rs");
