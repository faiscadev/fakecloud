//! AWS Elastic Beanstalk (`elasticbeanstalk`) Query-protocol service.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::query::{optional_query_param, query_response_xml, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::{SnapshotHook, SnapshotStore};

use crate::state::{
    environment_status as est, AccountState, Application, ApplicationVersion,
    ConfigurationTemplate, ElasticBeanstalkSnapshot, Environment, Event, MaxAgeRule, MaxCountRule,
    OptionSetting, ResourceLifecycleConfig, ResourceTag, SharedEbState, SourceBuildInformation,
    ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION,
};

const NS: &str = "http://elasticbeanstalk.amazonaws.com/docs/2010-12-01/";

/// Default managed-transition settle delay. Kept short so a Terraform
/// `aws_elastic_beanstalk_environment` create waiter (which polls every few
/// seconds for `Status=Ready`) completes promptly. Overridable via
/// `FAKECLOUD_EB_SETTLE_MS`.
const DEFAULT_SETTLE_MS: u64 = 800;

const SUPPORTED_ACTIONS: &[&str] = &[
    "AbortEnvironmentUpdate",
    "ApplyEnvironmentManagedAction",
    "AssociateEnvironmentOperationsRole",
    "CheckDNSAvailability",
    "ComposeEnvironments",
    "CreateApplication",
    "CreateApplicationVersion",
    "CreateConfigurationTemplate",
    "CreateEnvironment",
    "CreatePlatformVersion",
    "CreateStorageLocation",
    "DeleteApplication",
    "DeleteApplicationVersion",
    "DeleteConfigurationTemplate",
    "DeleteEnvironmentConfiguration",
    "DeletePlatformVersion",
    "DescribeAccountAttributes",
    "DescribeApplicationVersions",
    "DescribeApplications",
    "DescribeConfigurationOptions",
    "DescribeConfigurationSettings",
    "DescribeEnvironmentHealth",
    "DescribeEnvironmentManagedActionHistory",
    "DescribeEnvironmentManagedActions",
    "DescribeEnvironmentResources",
    "DescribeEnvironments",
    "DescribeEvents",
    "DescribeInstancesHealth",
    "DescribePlatformVersion",
    "DisassociateEnvironmentOperationsRole",
    "ListAvailableSolutionStacks",
    "ListPlatformBranches",
    "ListPlatformVersions",
    "ListTagsForResource",
    "RebuildEnvironment",
    "RequestEnvironmentInfo",
    "RestartAppServer",
    "RetrieveEnvironmentInfo",
    "SwapEnvironmentCNAMEs",
    "TerminateEnvironment",
    "UpdateApplication",
    "UpdateApplicationResourceLifecycle",
    "UpdateApplicationVersion",
    "UpdateConfigurationTemplate",
    "UpdateEnvironment",
    "UpdateTagsForResource",
    "ValidateConfigurationSettings",
];

/// Solution stacks Elastic Beanstalk offers. Mirrors the shape of the real
/// `ListAvailableSolutionStacks` response (newest platform versions per
/// language, most-recent first).
const SOLUTION_STACKS: &[&str] = &[
    "64bit Amazon Linux 2023 v6.1.2 running Node.js 20",
    "64bit Amazon Linux 2023 v6.1.2 running Node.js 18",
    "64bit Amazon Linux 2023 v4.3.2 running Corretto 21",
    "64bit Amazon Linux 2023 v4.3.2 running Corretto 17",
    "64bit Amazon Linux 2023 v4.3.2 running Corretto 11",
    "64bit Amazon Linux 2023 v4.3.1 running Go 1.21",
    "64bit Amazon Linux 2023 v4.2.2 running PHP 8.3",
    "64bit Amazon Linux 2023 v4.2.2 running PHP 8.2",
    "64bit Amazon Linux 2023 v4.1.2 running Python 3.11",
    "64bit Amazon Linux 2023 v4.1.2 running Python 3.9",
    "64bit Amazon Linux 2023 v4.1.2 running Ruby 3.2",
    "64bit Amazon Linux 2023 v4.3.0 running .NET 8",
    "64bit Amazon Linux 2 v4.1.0 running .NET Core",
    "64bit Amazon Linux 2 v3.8.0 running Docker",
    "64bit Amazon Linux 2 v4.4.0 running Tomcat 9 Corretto 11",
    "64bit Windows Server 2019 v2.16.5 running IIS 10.0",
    "64bit Windows Server Core 2019 v2.16.5 running IIS 10.0",
];

pub struct ElasticBeanstalkService {
    state: SharedEbState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    settle_ms: u64,
}

impl ElasticBeanstalkService {
    pub fn new(state: SharedEbState) -> Self {
        let settle_ms = std::env::var("FAKECLOUD_EB_SETTLE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SETTLE_MS);
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            settle_ms,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Override the managed-transition settle delay (tests use `0` for a
    /// synchronous-feeling settle).
    pub fn with_settle_ms(mut self, ms: u64) -> Self {
        self.settle_ms = ms;
        self
    }

    async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let bytes = {
            let snap = ElasticBeanstalkSnapshot {
                schema_version: ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION,
                accounts: Some(self.state.read().clone()),
            };
            serde_json::to_vec(&snap).unwrap_or_default()
        };
        let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
    }

    pub fn snapshot_hook(&self) -> Option<SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let store = store.clone();
            let state = state.clone();
            let lock = lock.clone();
            Box::pin(async move {
                let _guard = lock.lock().await;
                let bytes = {
                    let snap = ElasticBeanstalkSnapshot {
                        schema_version: ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION,
                        accounts: Some(state.read().clone()),
                    };
                    serde_json::to_vec(&snap).unwrap_or_default()
                };
                let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
            })
        }))
    }

    /// Re-drive any environment left mid-transition by a restart. A persisted
    /// environment survives a restart but its settle task does not, so an env
    /// still in `Launching` / `Updating` / `Terminating` would be stuck
    /// forever without this. Mirrors RDS / ElastiCache container recovery.
    pub fn recover_pending_environments(&self) {
        let pending: Vec<(String, String, String, String)> = {
            let guard = self.state.read();
            let mut out = Vec::new();
            for (account, acct) in &guard.accounts {
                for env in acct.environments.values() {
                    let target = match env.status.as_str() {
                        est::LAUNCHING | est::UPDATING => est::READY,
                        est::TERMINATING => est::TERMINATED,
                        _ => continue,
                    };
                    out.push((
                        account.clone(),
                        env.id.clone(),
                        env.status.clone(),
                        target.to_string(),
                    ));
                }
            }
            out
        };
        for (account, env_id, from, to) in pending {
            self.spawn_settle(account, env_id, from, to);
        }
    }

    /// Spawn the background task that settles an environment from a transient
    /// status (`Launching` / `Updating` / `Terminating`) into its terminal
    /// state, emitting the corresponding Event and persisting.
    fn spawn_settle(&self, account: String, env_id: String, from: String, to: String) {
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        let delay = self.settle_ms;
        tokio::spawn(async move {
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            {
                let mut guard = state.write();
                let Some(acct) = guard.accounts.get_mut(&account) else {
                    return;
                };
                let Some(env) = acct.environments.get_mut(&env_id) else {
                    return;
                };
                // Only settle if still in the transient state we were spawned
                // for; a newer operation may have moved it on.
                if env.status != from {
                    return;
                }
                let now = Utc::now();
                env.status = to.clone();
                env.date_updated = now;
                env.abortable_operation_in_progress = false;
                let (env_name, app_name) = (env.name.clone(), env.application_name.clone());
                let (message, health, health_status) = match to.as_str() {
                    est::READY => {
                        env.health = "Green".to_string();
                        env.health_status = "Ok".to_string();
                        (
                            format!("Successfully launched environment: {env_name}"),
                            "Green",
                            "Ok",
                        )
                    }
                    est::TERMINATED => {
                        env.health = "Grey".to_string();
                        env.health_status = "Suspended".to_string();
                        (
                            format!("Environment {env_name} successfully terminated."),
                            "Grey",
                            "Suspended",
                        )
                    }
                    _ => (String::new(), "Grey", "Unknown"),
                };
                let _ = (health, health_status);
                acct.events.insert(
                    0,
                    Event {
                        event_date: now,
                        message,
                        application_name: Some(app_name),
                        version_label: env.version_label.clone(),
                        template_name: None,
                        environment_name: Some(env_name),
                        platform_arn: env.platform_arn.clone(),
                        request_id: Some(Uuid::new_v4().to_string()),
                        severity: "INFO".to_string(),
                    },
                );
            }
            if let Some(store) = store {
                let _guard = lock.lock().await;
                let bytes = {
                    let snap = ElasticBeanstalkSnapshot {
                        schema_version: ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION,
                        accounts: Some(state.read().clone()),
                    };
                    serde_json::to_vec(&snap).unwrap_or_default()
                };
                let _ = tokio::task::spawn_blocking(move || store.save(&bytes)).await;
            }
        });
    }

    fn ok(&self, action: &str, inner: String, req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(action, NS, &inner, &req.request_id),
        )
    }
}

// ---------------------------------------------------------------------------
// XML + parsing helpers
// ---------------------------------------------------------------------------

fn xesc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Emit `<Name>value</Name>`.
fn el(name: &str, value: &str) -> String {
    format!("<{name}>{}</{name}>", xesc(value))
}

/// Emit `<Name>value</Name>` only when `value` is `Some`.
fn opt_el(name: &str, value: &Option<String>) -> String {
    match value {
        Some(v) => el(name, v),
        None => String::new(),
    }
}

fn iso(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Parse `Prefix.member.N` (and the `Prefix.N` variant) into a list.
fn member_list(req: &AwsRequest, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for n in 1..=200 {
        let v = req
            .query_params
            .get(&format!("{prefix}.member.{n}"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}")));
        match v {
            Some(val) => out.push(val.clone()),
            None => break,
        }
    }
    out
}

/// Parse a `ConfigurationOptionSettingsList` (`Prefix.member.N.{Namespace,
/// OptionName,Value,ResourceName}`).
fn parse_option_settings(req: &AwsRequest, prefix: &str) -> Vec<OptionSetting> {
    let mut out = Vec::new();
    for n in 1..=500 {
        let ns = req
            .query_params
            .get(&format!("{prefix}.member.{n}.Namespace"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}.Namespace")));
        let name = req
            .query_params
            .get(&format!("{prefix}.member.{n}.OptionName"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}.OptionName")));
        let (Some(ns), Some(name)) = (ns, name) else {
            break;
        };
        let value = req
            .query_params
            .get(&format!("{prefix}.member.{n}.Value"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}.Value")))
            .cloned();
        let resource_name = req
            .query_params
            .get(&format!("{prefix}.member.{n}.ResourceName"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}.ResourceName")))
            .cloned();
        out.push(OptionSetting {
            resource_name,
            namespace: ns.clone(),
            option_name: name.clone(),
            value,
        });
    }
    out
}

/// Parse a `Tags.member.N.{Key,Value}` list.
fn parse_tags(req: &AwsRequest, prefix: &str) -> Vec<ResourceTag> {
    let mut out = Vec::new();
    for n in 1..=200 {
        let key = req
            .query_params
            .get(&format!("{prefix}.member.{n}.Key"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}.Key")));
        let Some(key) = key else { break };
        let value = req
            .query_params
            .get(&format!("{prefix}.member.{n}.Value"))
            .or_else(|| req.query_params.get(&format!("{prefix}.{n}.Value")))
            .cloned()
            .unwrap_or_default();
        out.push(ResourceTag {
            key: key.clone(),
            value,
        });
    }
    out
}

fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterValue", msg.into())
}

fn missing_parameter(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MissingParameter",
        format!("The request must contain the parameter {name}."),
    )
}

/// Enforce a Smithy `@length` bound on a string, returning `InvalidParameterValue`.
fn check_len(name: &str, value: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    let len = value.chars().count();
    if len < min || len > max {
        return Err(invalid_parameter(format!(
            "Value '{value}' at '{name}' failed to satisfy constraint: Member must have length between {min} and {max}."
        )));
    }
    Ok(())
}

/// Model-derived `@length` bounds for the string input members shared across
/// operations (each parameter carries the same constraint everywhere it
/// appears in the Smithy model).
const STRING_LENGTH_BOUNDS: &[(&str, usize, usize)] = &[
    ("ApplicationName", 1, 100),
    ("EnvironmentName", 4, 40),
    ("TemplateName", 1, 100),
    ("VersionLabel", 1, 100),
    ("Description", 0, 200),
    ("GroupName", 1, 19),
    ("CNAMEPrefix", 4, 63),
    ("OperationsRole", 1, 256),
    ("NextToken", 1, 100),
];

/// Validate the constrained input members present on a request against their
/// Smithy `@length` / `@range` / `@enum` traits, returning
/// `InvalidParameterValue` on violation. Only members actually present are
/// checked, so well-formed requests pass untouched.
fn validate_params(req: &AwsRequest) -> Result<(), AwsServiceError> {
    for (name, min, max) in STRING_LENGTH_BOUNDS {
        if let Some(value) = req.query_params.get(*name) {
            // Any present value is length-checked. A zero-min member
            // (Description) accepts empty; a `min >= 1` member rejects empty
            // as a length violation, exactly as the live API does.
            check_len(name, value, *min, *max)?;
        }
    }
    // MaxRecords: `@range` min is 1 everywhere; the max is 1000 for the
    // Describe* pagers and unbounded for the platform listers.
    if let Some(v) = req.query_params.get("MaxRecords") {
        if let Ok(n) = v.parse::<i64>() {
            let max = if matches!(
                req.action.as_str(),
                "ListPlatformVersions" | "ListPlatformBranches"
            ) {
                i64::MAX
            } else {
                1000
            };
            if n < 1 || n > max {
                return Err(invalid_parameter(format!(
                    "Value '{n}' at 'maxRecords' failed to satisfy constraint: Member must be between 1 and {max}."
                )));
            }
        }
    }
    // MaxItems on DescribeEnvironmentManagedActionHistory: `@range` 1..=100.
    if let Some(v) = req.query_params.get("MaxItems") {
        if let Ok(n) = v.parse::<i64>() {
            if !(1..=100).contains(&n) {
                return Err(invalid_parameter(format!(
                    "Value '{n}' at 'maxItems' failed to satisfy constraint: Member must be between 1 and 100."
                )));
            }
        }
    }
    // Severity on DescribeEvents is the `EventSeverity` enum.
    if let Some(v) = req.query_params.get("Severity") {
        const EVENT_SEVERITY: &[&str] = &["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"];
        if !EVENT_SEVERITY.contains(&v.as_str()) {
            return Err(invalid_parameter(format!(
                "Value '{v}' at 'severity' failed to satisfy constraint: Member must satisfy enum value set: [TRACE, DEBUG, INFO, WARN, ERROR, FATAL]."
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ARN + identifier helpers
// ---------------------------------------------------------------------------

fn application_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:elasticbeanstalk:{region}:{account}:application/{name}")
}

fn application_version_arn(region: &str, account: &str, app: &str, label: &str) -> String {
    format!("arn:aws:elasticbeanstalk:{region}:{account}:applicationversion/{app}/{label}")
}

fn environment_arn(region: &str, account: &str, app: &str, env: &str) -> String {
    format!("arn:aws:elasticbeanstalk:{region}:{account}:environment/{app}/{env}")
}

fn gen_environment_id() -> String {
    let hex = Uuid::new_v4().simple().to_string();
    format!("e-{}", &hex[..10])
}

fn storage_bucket_name(region: &str, account: &str) -> String {
    format!("elasticbeanstalk-{region}-{account}")
}

/// Build the modern Elastic Beanstalk CNAME
/// (`<prefix>.<hash>.<region>.elasticbeanstalk.com`).
fn build_cname(prefix: &str, region: &str) -> String {
    let hash = Uuid::new_v4().simple().to_string();
    format!("{prefix}.{}.{region}.elasticbeanstalk.com", &hash[..8])
}

/// Build the ELB-style endpoint URL for a web-server tier environment. The
/// `AWSEBLoa` fragment mirrors the real Elastic Beanstalk load-balancer DNS
/// name (`awseb-<id>-AWSEBLoa<hash>-<n>.<region>.elb.amazonaws.com`).
fn build_endpoint_url(env_id: &str, region: &str) -> String {
    let hash = Uuid::new_v4().simple().to_string();
    format!(
        "awseb-{env_id}-AWSEBLoa{}-{}.{region}.elb.amazonaws.com",
        &hash[..8].to_uppercase(),
        &hash[8..16]
    )
}

/// Deterministic uppercase suffix for the synthesized environment resource
/// names, derived from the environment id so repeated `DescribeEnvironmentResources`
/// (and Terraform's import-verify re-read) return identical values.
fn resource_suffix(env_id: &str) -> String {
    let hex: String = env_id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    format!("{hex:0<13}").to_uppercase()[..13].to_string()
}

/// Synthesized EC2 instance id for a settled environment (deterministic).
fn resource_instance_id(env_id: &str) -> String {
    let hex: String = env_id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    format!("i-{hex:0<17}")[..19].to_string()
}

// ---------------------------------------------------------------------------
// Application handlers
// ---------------------------------------------------------------------------

impl ElasticBeanstalkService {
    fn create_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "ApplicationName")?;
        check_len("ApplicationName", &name, 1, 100)?;
        let description = optional_query_param(req, "Description");
        let lifecycle = parse_resource_lifecycle_config(req, "ResourceLifecycleConfig");
        let tags = parse_tags(req, "Tags");
        let now = Utc::now();
        let arn = application_arn(&req.region, &req.account_id, &name);

        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let app = acct
            .applications
            .entry(name.clone())
            .or_insert_with(|| Application {
                name: name.clone(),
                arn: arn.clone(),
                description: description.clone(),
                date_created: now,
                date_updated: now,
                resource_lifecycle_config: lifecycle.clone().unwrap_or_default(),
            });
        // Idempotent-ish: if the application already existed, echo it back.
        let app = app.clone();
        if !tags.is_empty() {
            acct.tags.insert(arn.clone(), tags);
        }
        let inner = format!("<Application>{}</Application>", render_application(&app));
        Ok(self.ok("CreateApplication", inner, req))
    }

    fn update_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "ApplicationName")?;
        let description = optional_query_param(req, "Description");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(app) = acct.applications.get_mut(&name) else {
            return Err(invalid_parameter(format!(
                "No Application named '{name}' found."
            )));
        };
        if req.query_params.contains_key("Description") {
            app.description = description;
        }
        app.date_updated = Utc::now();
        let app = app.clone();
        let inner = format!("<Application>{}</Application>", render_application(&app));
        Ok(self.ok("UpdateApplication", inner, req))
    }

    fn update_application_resource_lifecycle(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "ApplicationName")?;
        let lifecycle = parse_resource_lifecycle_config(req, "ResourceLifecycleConfig")
            .ok_or_else(|| missing_parameter("ResourceLifecycleConfig"))?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(app) = acct.applications.get_mut(&name) else {
            return Err(invalid_parameter(format!(
                "No Application named '{name}' found."
            )));
        };
        app.resource_lifecycle_config = lifecycle.clone();
        app.date_updated = Utc::now();
        let inner = format!(
            "{}{}",
            el("ApplicationName", &name),
            render_resource_lifecycle_config(&lifecycle)
        );
        Ok(self.ok("UpdateApplicationResourceLifecycle", inner, req))
    }

    fn describe_applications(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let filter = member_list(req, "ApplicationNames");
        let guard = self.state.read();
        let apps: Vec<String> = match guard.accounts.get(&req.account_id) {
            Some(acct) => acct
                .applications
                .values()
                .filter(|a| filter.is_empty() || filter.contains(&a.name))
                .map(|a| render_application_with_children(a, acct))
                .collect(),
            None => Vec::new(),
        };
        let inner = format!("<Applications>{}</Applications>", wrap_members(&apps));
        Ok(self.ok("DescribeApplications", inner, req))
    }

    fn delete_application(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "ApplicationName")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        acct.applications.remove(&name);
        acct.versions.retain(|(app, _), _| app != &name);
        acct.templates.retain(|(app, _), _| app != &name);
        // Environments of the application move to Terminating (settled async).
        let to_settle: Vec<String> = acct
            .environments
            .values_mut()
            .filter(|e| e.application_name == name && e.status != est::TERMINATED)
            .map(|e| {
                e.status = est::TERMINATING.to_string();
                e.abortable_operation_in_progress = false;
                e.id.clone()
            })
            .collect();
        drop(guard);
        for id in to_settle {
            self.spawn_settle(
                req.account_id.clone(),
                id,
                est::TERMINATING.to_string(),
                est::TERMINATED.to_string(),
            );
        }
        Ok(self.ok_empty("DeleteApplication", req))
    }
}

// ---------------------------------------------------------------------------
// Application version handlers
// ---------------------------------------------------------------------------

impl ElasticBeanstalkService {
    fn create_application_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let label = required_query_param(req, "VersionLabel")?;
        check_len("VersionLabel", &label, 1, 100)?;
        let description = optional_query_param(req, "Description");
        let auto_create = optional_query_param(req, "AutoCreateApplication")
            .map(|v| v == "true")
            .unwrap_or(false);
        let bucket = optional_query_param(req, "SourceBundle.S3Bucket");
        let key = optional_query_param(req, "SourceBundle.S3Key");
        let source_build = parse_source_build_information(req);
        let now = Utc::now();

        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.applications.contains_key(&app_name) {
            if auto_create {
                let arn = application_arn(&req.region, &req.account_id, &app_name);
                acct.applications.insert(
                    app_name.clone(),
                    Application {
                        name: app_name.clone(),
                        arn,
                        description: None,
                        date_created: now,
                        date_updated: now,
                        resource_lifecycle_config: ResourceLifecycleConfig::default(),
                    },
                );
            } else {
                return Err(invalid_parameter(format!(
                    "No Application named '{app_name}' found."
                )));
            }
        }
        let arn = application_version_arn(&req.region, &req.account_id, &app_name, &label);
        let version = ApplicationVersion {
            application_name: app_name.clone(),
            version_label: label.clone(),
            arn: arn.clone(),
            description,
            source_bundle_bucket: bucket,
            source_bundle_key: key,
            source_build_information: source_build,
            build_arn: None,
            date_created: now,
            date_updated: now,
            status: "Processed".to_string(),
        };
        acct.versions
            .insert((app_name.clone(), label.clone()), version.clone());
        let tags = parse_tags(req, "Tags");
        if !tags.is_empty() {
            acct.tags.insert(arn, tags);
        }
        let inner = format!(
            "<ApplicationVersion>{}</ApplicationVersion>",
            render_application_version(&version)
        );
        Ok(self.ok("CreateApplicationVersion", inner, req))
    }

    fn update_application_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let label = required_query_param(req, "VersionLabel")?;
        let description = optional_query_param(req, "Description");
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(version) = acct.versions.get_mut(&(app_name.clone(), label.clone())) else {
            return Err(invalid_parameter(format!(
                "No Application Version named '{label}' found for application '{app_name}'."
            )));
        };
        if req.query_params.contains_key("Description") {
            version.description = description;
        }
        version.date_updated = Utc::now();
        let version = version.clone();
        let inner = format!(
            "<ApplicationVersion>{}</ApplicationVersion>",
            render_application_version(&version)
        );
        Ok(self.ok("UpdateApplicationVersion", inner, req))
    }

    fn describe_application_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_filter = optional_query_param(req, "ApplicationName");
        let label_filter = member_list(req, "VersionLabels");
        let guard = self.state.read();
        let versions: Vec<String> = match guard.accounts.get(&req.account_id) {
            Some(acct) => acct
                .versions
                .values()
                .filter(|v| {
                    app_filter
                        .as_ref()
                        .map(|a| a == &v.application_name)
                        .unwrap_or(true)
                })
                .filter(|v| label_filter.is_empty() || label_filter.contains(&v.version_label))
                .map(render_application_version)
                .collect(),
            None => Vec::new(),
        };
        let inner = format!(
            "<ApplicationVersions>{}</ApplicationVersions>",
            wrap_members(&versions)
        );
        Ok(self.ok("DescribeApplicationVersions", inner, req))
    }

    fn delete_application_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let label = required_query_param(req, "VersionLabel")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        acct.versions.remove(&(app_name, label));
        Ok(self.ok_empty("DeleteApplicationVersion", req))
    }
}

// ---------------------------------------------------------------------------
// Environment handlers
// ---------------------------------------------------------------------------

impl ElasticBeanstalkService {
    async fn create_environment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let env_name = optional_query_param(req, "EnvironmentName");
        if let Some(ref n) = env_name {
            check_len("EnvironmentName", n, 4, 40)?;
        }
        let group_name = optional_query_param(req, "GroupName");
        // Either EnvironmentName or (GroupName+template) is required by AWS.
        let env_name = match env_name {
            Some(n) => n,
            None => {
                return Err(missing_parameter("EnvironmentName"));
            }
        };
        let solution_stack = optional_query_param(req, "SolutionStackName");
        let platform_arn = optional_query_param(req, "PlatformArn");
        let template_name = optional_query_param(req, "TemplateName");
        let version_label = optional_query_param(req, "VersionLabel");
        let description = optional_query_param(req, "Description");
        let operations_role = optional_query_param(req, "OperationsRole");
        let cname_prefix =
            optional_query_param(req, "CNAMEPrefix").unwrap_or_else(|| env_name.clone());
        if let Some(prefix) = optional_query_param(req, "CNAMEPrefix") {
            check_len("CNAMEPrefix", &prefix, 4, 63)?;
        }
        let option_settings = parse_option_settings(req, "OptionSettings");
        let tags = parse_tags(req, "Tags");
        let (tier_name, tier_type, tier_version) = parse_tier(req);

        let now = Utc::now();
        let env_id = gen_environment_id();
        let cname = build_cname(&cname_prefix, &req.region);
        let endpoint_url = build_endpoint_url(&env_id, &req.region);
        let arn = environment_arn(&req.region, &req.account_id, &app_name, &env_name);

        let rendered = {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            if !acct.applications.contains_key(&app_name) {
                return Err(invalid_parameter(format!(
                    "No Application named '{app_name}' found."
                )));
            }
            if acct
                .environments
                .values()
                .any(|e| e.name == env_name && e.status != est::TERMINATED)
            {
                return Err(invalid_parameter(format!(
                    "Environment {env_name} already exists."
                )));
            }
            let env = Environment {
                name: env_name.clone(),
                id: env_id.clone(),
                arn: arn.clone(),
                application_name: app_name.clone(),
                version_label,
                solution_stack_name: solution_stack.clone(),
                platform_arn,
                template_name,
                description,
                cname,
                endpoint_url,
                date_created: now,
                date_updated: now,
                status: est::LAUNCHING.to_string(),
                abortable_operation_in_progress: true,
                health: "Grey".to_string(),
                health_status: "Pending".to_string(),
                tier_name,
                tier_type,
                tier_version,
                operations_role,
                group_name,
                option_settings,
            };
            let rendered = render_environment(&env);
            acct.environments.insert(env_id.clone(), env);
            if !tags.is_empty() {
                acct.tags.insert(arn, tags);
            }
            acct.events.insert(
                0,
                Event {
                    event_date: now,
                    message: format!("createEnvironment is starting for {env_name}."),
                    application_name: Some(app_name.clone()),
                    version_label: None,
                    template_name: None,
                    environment_name: Some(env_name.clone()),
                    platform_arn: None,
                    request_id: Some(req.request_id.clone()),
                    severity: "INFO".to_string(),
                },
            );
            rendered
        };
        self.spawn_settle(
            req.account_id.clone(),
            env_id.clone(),
            est::LAUNCHING.to_string(),
            est::READY.to_string(),
        );
        let resp = self.ok("CreateEnvironment", rendered, req);
        self.save_snapshot().await;
        Ok(resp)
    }

    async fn update_environment(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let env_id = self.resolve_environment_id(req)?;
        let version_label = optional_query_param(req, "VersionLabel");
        let description = optional_query_param(req, "Description");
        let solution_stack = optional_query_param(req, "SolutionStackName");
        let platform_arn = optional_query_param(req, "PlatformArn");
        let template_name = optional_query_param(req, "TemplateName");
        let new_settings = parse_option_settings(req, "OptionSettings");
        let remove = parse_options_to_remove(req);

        let rendered;
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            let Some(env) = acct.environments.get_mut(&env_id) else {
                return Err(invalid_parameter("No Environment found.".to_string()));
            };
            if req.query_params.contains_key("VersionLabel") {
                env.version_label = version_label;
            }
            if req.query_params.contains_key("Description") {
                env.description = description;
            }
            if solution_stack.is_some() {
                env.solution_stack_name = solution_stack;
            }
            if platform_arn.is_some() {
                env.platform_arn = platform_arn;
            }
            if template_name.is_some() {
                env.template_name = template_name;
            }
            apply_option_changes(&mut env.option_settings, &new_settings, &remove);
            env.status = est::UPDATING.to_string();
            env.abortable_operation_in_progress = true;
            env.date_updated = Utc::now();
            let (env_name, app_name) = (env.name.clone(), env.application_name.clone());
            rendered = render_environment(env);
            acct.events.insert(
                0,
                Event {
                    event_date: Utc::now(),
                    message: format!("Environment update is starting for {env_name}."),
                    application_name: Some(app_name),
                    version_label: env.version_label.clone(),
                    template_name: None,
                    environment_name: Some(env_name),
                    platform_arn: None,
                    request_id: Some(req.request_id.clone()),
                    severity: "INFO".to_string(),
                },
            );
        }
        self.spawn_settle(
            req.account_id.clone(),
            env_id,
            est::UPDATING.to_string(),
            est::READY.to_string(),
        );
        let resp = self.ok("UpdateEnvironment", rendered, req);
        self.save_snapshot().await;
        Ok(resp)
    }

    async fn terminate_environment(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let env_id = self.resolve_environment_id(req)?;
        let rendered;
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            let Some(env) = acct.environments.get_mut(&env_id) else {
                return Err(invalid_parameter("No Environment found.".to_string()));
            };
            env.status = est::TERMINATING.to_string();
            env.abortable_operation_in_progress = false;
            env.health = "Grey".to_string();
            env.health_status = "Info".to_string();
            env.date_updated = Utc::now();
            let (env_name, app_name) = (env.name.clone(), env.application_name.clone());
            rendered = render_environment(env);
            acct.events.insert(
                0,
                Event {
                    event_date: Utc::now(),
                    message: format!("terminateEnvironment is starting for {env_name}."),
                    application_name: Some(app_name),
                    version_label: None,
                    template_name: None,
                    environment_name: Some(env_name),
                    platform_arn: None,
                    request_id: Some(req.request_id.clone()),
                    severity: "INFO".to_string(),
                },
            );
        }
        self.spawn_settle(
            req.account_id.clone(),
            env_id,
            est::TERMINATING.to_string(),
            est::TERMINATED.to_string(),
        );
        let resp = self.ok("TerminateEnvironment", rendered, req);
        self.save_snapshot().await;
        Ok(resp)
    }

    fn describe_environments(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let app_filter = optional_query_param(req, "ApplicationName");
        let version_filter = optional_query_param(req, "VersionLabel");
        let id_filter = member_list(req, "EnvironmentIds");
        let name_filter = member_list(req, "EnvironmentNames");
        let include_deleted = optional_query_param(req, "IncludeDeleted")
            .map(|v| v == "true")
            .unwrap_or(false);
        let guard = self.state.read();
        let envs: Vec<String> = match guard.accounts.get(&req.account_id) {
            Some(acct) => acct
                .environments
                .values()
                .filter(|e| include_deleted || e.status != est::TERMINATED)
                .filter(|e| {
                    app_filter
                        .as_ref()
                        .map(|a| a == &e.application_name)
                        .unwrap_or(true)
                })
                .filter(|e| {
                    version_filter
                        .as_ref()
                        .map(|v| e.version_label.as_deref() == Some(v.as_str()))
                        .unwrap_or(true)
                })
                .filter(|e| id_filter.is_empty() || id_filter.contains(&e.id))
                .filter(|e| name_filter.is_empty() || name_filter.contains(&e.name))
                .map(render_environment)
                .collect(),
            None => Vec::new(),
        };
        let inner = format!("<Environments>{}</Environments>", wrap_members(&envs));
        Ok(self.ok("DescribeEnvironments", inner, req))
    }

    fn describe_environment_resources(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let env_id = self.resolve_environment_id(req)?;
        let guard = self.state.read();
        let acct = guard
            .accounts
            .get(&req.account_id)
            .ok_or_else(|| invalid_parameter("No Environment found.".to_string()))?;
        let env = acct
            .environments
            .get(&env_id)
            .ok_or_else(|| invalid_parameter("No Environment found.".to_string()))?;
        // A terminated environment has torn down its resources; a live one
        // (web-server tier) has an Auto Scaling group, a launch
        // configuration, an instance, and a load balancer. Names mirror the
        // real `awseb-<id>-stack-AWSEB*` shapes and are deterministic.
        let inner = if env.status == est::TERMINATED {
            format!(
                "<EnvironmentResources>{}<AutoScalingGroups/><Instances/><LaunchConfigurations/><LaunchTemplates/><LoadBalancers/><Triggers/><Queues/></EnvironmentResources>",
                el("EnvironmentName", &env.name),
            )
        } else {
            let suffix = resource_suffix(&env.id);
            let asg = format!("awseb-{}-stack-AWSEBAutoScalingGroup-{suffix}", env.id);
            let lc = format!(
                "awseb-{}-stack-AWSEBAutoScalingLaunchConfiguration-{suffix}",
                env.id
            );
            let lb = format!("awseb-{}-stack-AWSEBLoadBalancer-{suffix}", env.id);
            let instance = resource_instance_id(&env.id);
            format!(
                "<EnvironmentResources>{}\
                 <AutoScalingGroups><member>{}</member></AutoScalingGroups>\
                 <Instances><member>{}</member></Instances>\
                 <LaunchConfigurations><member>{}</member></LaunchConfigurations>\
                 <LaunchTemplates/>\
                 <LoadBalancers><member>{}</member></LoadBalancers>\
                 <Triggers/><Queues/></EnvironmentResources>",
                el("EnvironmentName", &env.name),
                el("Name", &asg),
                el("Id", &instance),
                el("Name", &lc),
                el("Name", &lb),
            )
        };
        Ok(self.ok("DescribeEnvironmentResources", inner, req))
    }

    fn describe_environment_health(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let env_id = self.resolve_environment_id(req)?;
        let guard = self.state.read();
        let acct = guard
            .accounts
            .get(&req.account_id)
            .ok_or_else(|| invalid_parameter("No Environment found.".to_string()))?;
        let env = acct
            .environments
            .get(&env_id)
            .ok_or_else(|| invalid_parameter("No Environment found.".to_string()))?;
        let inner = format!(
            "{}{}{}{}<Causes/>{}",
            el("EnvironmentName", &env.name),
            el("HealthStatus", &env.health_status),
            el("Status", &env.status),
            el("Color", &env.health),
            el("RefreshedAt", &iso(Utc::now())),
        );
        Ok(self.ok("DescribeEnvironmentHealth", inner, req))
    }

    fn describe_instances_health(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _ = self.resolve_environment_id(req)?;
        let inner = format!(
            "<InstanceHealthList/>{}",
            el("RefreshedAt", &iso(Utc::now()))
        );
        Ok(self.ok("DescribeInstancesHealth", inner, req))
    }

    /// Simple environment-mutating ops that emit an event and stay/return
    /// their current shape without a lifecycle transition (RestartAppServer)
    /// or with a brief Updating settle (RebuildEnvironment).
    async fn environment_action(
        &self,
        req: &AwsRequest,
        action: &str,
        message_verb: &str,
        rebuild: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let env_id = self.resolve_environment_id(req)?;
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            let Some(env) = acct.environments.get_mut(&env_id) else {
                return Err(invalid_parameter("No Environment found.".to_string()));
            };
            let (env_name, app_name) = (env.name.clone(), env.application_name.clone());
            if rebuild {
                env.status = est::UPDATING.to_string();
                env.abortable_operation_in_progress = false;
                env.date_updated = Utc::now();
            }
            acct.events.insert(
                0,
                Event {
                    event_date: Utc::now(),
                    message: format!("{message_verb} for {env_name}."),
                    application_name: Some(app_name),
                    version_label: None,
                    template_name: None,
                    environment_name: Some(env_name),
                    platform_arn: None,
                    request_id: Some(req.request_id.clone()),
                    severity: "INFO".to_string(),
                },
            );
        }
        if rebuild {
            self.spawn_settle(
                req.account_id.clone(),
                env_id,
                est::UPDATING.to_string(),
                est::READY.to_string(),
            );
        }
        let resp = self.ok_empty(action, req);
        self.save_snapshot().await;
        Ok(resp)
    }

    async fn abort_environment_update(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let env_id = self.resolve_environment_id(req)?;
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            if let Some(env) = acct.environments.get_mut(&env_id) {
                if env.status == est::UPDATING {
                    env.status = est::READY.to_string();
                    env.abortable_operation_in_progress = false;
                    env.date_updated = Utc::now();
                }
            }
        }
        let resp = self.ok_empty("AbortEnvironmentUpdate", req);
        self.save_snapshot().await;
        Ok(resp)
    }

    fn associate_operations_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let env_name = required_query_param(req, "EnvironmentName")?;
        let role = required_query_param(req, "OperationsRole")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let env = acct
            .environments
            .values_mut()
            .find(|e| e.name == env_name && e.status != est::TERMINATED)
            .ok_or_else(|| {
                invalid_parameter(format!(
                    "No Environment found for EnvironmentName = '{env_name}'."
                ))
            })?;
        env.operations_role = Some(role);
        Ok(self.ok_empty("AssociateEnvironmentOperationsRole", req))
    }

    fn disassociate_operations_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let env_name = required_query_param(req, "EnvironmentName")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let env = acct
            .environments
            .values_mut()
            .find(|e| e.name == env_name && e.status != est::TERMINATED)
            .ok_or_else(|| {
                invalid_parameter(format!(
                    "No Environment found for EnvironmentName = '{env_name}'."
                ))
            })?;
        env.operations_role = None;
        Ok(self.ok_empty("DisassociateEnvironmentOperationsRole", req))
    }

    fn swap_environment_cnames(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let source = optional_query_param(req, "SourceEnvironmentId")
            .or_else(|| optional_query_param(req, "SourceEnvironmentName"));
        let dest = optional_query_param(req, "DestinationEnvironmentId")
            .or_else(|| optional_query_param(req, "DestinationEnvironmentName"));
        let (Some(source), Some(dest)) = (source, dest) else {
            return Err(missing_parameter("SourceEnvironmentName"));
        };
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let find_id = |acct: &AccountState, key: &str| -> Option<String> {
            acct.environments
                .values()
                .find(|e| e.id == key || e.name == key)
                .map(|e| e.id.clone())
        };
        let sid = find_id(acct, &source);
        let did = find_id(acct, &dest);
        let (Some(sid), Some(did)) = (sid, did) else {
            return Err(invalid_parameter("No Environment found.".to_string()));
        };
        let s_cname = acct.environments.get(&sid).map(|e| e.cname.clone());
        let d_cname = acct.environments.get(&did).map(|e| e.cname.clone());
        let s_ep = acct.environments.get(&sid).map(|e| e.endpoint_url.clone());
        let d_ep = acct.environments.get(&did).map(|e| e.endpoint_url.clone());
        if let (Some(sc), Some(dc), Some(se), Some(de)) = (s_cname, d_cname, s_ep, d_ep) {
            if let Some(e) = acct.environments.get_mut(&sid) {
                e.cname = dc;
                e.endpoint_url = de;
                e.date_updated = Utc::now();
            }
            if let Some(e) = acct.environments.get_mut(&did) {
                e.cname = sc;
                e.endpoint_url = se;
                e.date_updated = Utc::now();
            }
        }
        Ok(self.ok_empty("SwapEnvironmentCNAMEs", req))
    }

    fn compose_environments(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ComposeEnvironments groups already-created environments; with no new
        // environments to launch it returns the matching environment set.
        let app_filter = optional_query_param(req, "ApplicationName");
        let group = optional_query_param(req, "GroupName");
        let guard = self.state.read();
        let envs: Vec<String> = match guard.accounts.get(&req.account_id) {
            Some(acct) => acct
                .environments
                .values()
                .filter(|e| e.status != est::TERMINATED)
                .filter(|e| {
                    app_filter
                        .as_ref()
                        .map(|a| a == &e.application_name)
                        .unwrap_or(true)
                })
                .filter(|e| {
                    group
                        .as_ref()
                        .map(|g| e.group_name.as_deref() == Some(g))
                        .unwrap_or(true)
                })
                .map(render_environment)
                .collect(),
            None => Vec::new(),
        };
        let inner = format!("<Environments>{}</Environments>", wrap_members(&envs));
        Ok(self.ok("ComposeEnvironments", inner, req))
    }

    fn request_environment_info(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        required_query_param(req, "InfoType")?;
        let _ = self.resolve_environment_id(req)?;
        Ok(self.ok_empty("RequestEnvironmentInfo", req))
    }

    fn retrieve_environment_info(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        required_query_param(req, "InfoType")?;
        let _ = self.resolve_environment_id(req)?;
        Ok(self.ok(
            "RetrieveEnvironmentInfo",
            "<EnvironmentInfo/>".to_string(),
            req,
        ))
    }

    fn delete_environment_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        required_query_param(req, "ApplicationName")?;
        required_query_param(req, "EnvironmentName")?;
        Ok(self.ok_empty("DeleteEnvironmentConfiguration", req))
    }

    /// Resolve an environment id from either `EnvironmentId` or
    /// `EnvironmentName`.
    fn resolve_environment_id(&self, req: &AwsRequest) -> Result<String, AwsServiceError> {
        if let Some(id) = optional_query_param(req, "EnvironmentId") {
            let guard = self.state.read();
            if let Some(acct) = guard.accounts.get(&req.account_id) {
                if acct.environments.contains_key(&id) {
                    return Ok(id);
                }
            }
            return Err(invalid_parameter(format!(
                "No Environment found for EnvironmentId = '{id}'."
            )));
        }
        if let Some(name) = optional_query_param(req, "EnvironmentName") {
            let guard = self.state.read();
            if let Some(acct) = guard.accounts.get(&req.account_id) {
                if let Some(env) = acct
                    .environments
                    .values()
                    .find(|e| e.name == name && e.status != est::TERMINATED)
                {
                    return Ok(env.id.clone());
                }
            }
            return Err(invalid_parameter(format!(
                "No Environment found for EnvironmentName = '{name}'."
            )));
        }
        Err(missing_parameter("EnvironmentName"))
    }
}

// ---------------------------------------------------------------------------
// Configuration template / settings / options handlers
// ---------------------------------------------------------------------------

impl ElasticBeanstalkService {
    fn create_configuration_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let template_name = required_query_param(req, "TemplateName")?;
        check_len("TemplateName", &template_name, 1, 100)?;
        let solution_stack = optional_query_param(req, "SolutionStackName");
        let platform_arn = optional_query_param(req, "PlatformArn");
        let description = optional_query_param(req, "Description");
        let environment_name = optional_query_param(req, "EnvironmentName");
        let option_settings = parse_option_settings(req, "OptionSettings");
        let now = Utc::now();
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        if !acct.applications.contains_key(&app_name) {
            return Err(invalid_parameter(format!(
                "No Application named '{app_name}' found."
            )));
        }
        let template = ConfigurationTemplate {
            application_name: app_name.clone(),
            template_name: template_name.clone(),
            description,
            solution_stack_name: solution_stack,
            platform_arn,
            environment_name,
            deployment_status: "deployed".to_string(),
            date_created: now,
            date_updated: now,
            option_settings,
        };
        acct.templates
            .insert((app_name, template_name), template.clone());
        let inner = render_configuration_settings(&template);
        Ok(self.ok("CreateConfigurationTemplate", inner, req))
    }

    fn update_configuration_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let template_name = required_query_param(req, "TemplateName")?;
        let description = optional_query_param(req, "Description");
        let new_settings = parse_option_settings(req, "OptionSettings");
        let remove = parse_options_to_remove(req);
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        let Some(template) = acct
            .templates
            .get_mut(&(app_name.clone(), template_name.clone()))
        else {
            return Err(invalid_parameter(format!(
                "No Configuration Template named '{template_name}' found."
            )));
        };
        if req.query_params.contains_key("Description") {
            template.description = description;
        }
        apply_option_changes(&mut template.option_settings, &new_settings, &remove);
        template.date_updated = Utc::now();
        let template = template.clone();
        let inner = render_configuration_settings(&template);
        Ok(self.ok("UpdateConfigurationTemplate", inner, req))
    }

    fn delete_configuration_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let template_name = required_query_param(req, "TemplateName")?;
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        acct.templates.remove(&(app_name, template_name));
        Ok(self.ok_empty("DeleteConfigurationTemplate", req))
    }

    fn describe_configuration_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let app_name = required_query_param(req, "ApplicationName")?;
        let template_name = optional_query_param(req, "TemplateName");
        let env_name = optional_query_param(req, "EnvironmentName");
        let guard = self.state.read();
        let mut items: Vec<String> = Vec::new();
        if let Some(acct) = guard.accounts.get(&req.account_id) {
            if let Some(tn) = &template_name {
                if let Some(t) = acct.templates.get(&(app_name.clone(), tn.clone())) {
                    items.push(render_configuration_settings(t));
                }
            } else if let Some(en) = &env_name {
                if let Some(env) = acct
                    .environments
                    .values()
                    .find(|e| e.name == *en && e.application_name == app_name)
                {
                    items.push(render_environment_settings(env));
                }
            }
        }
        let inner = format!(
            "<ConfigurationSettings>{}</ConfigurationSettings>",
            wrap_members(&items)
        );
        Ok(self.ok("DescribeConfigurationSettings", inner, req))
    }

    fn describe_configuration_options(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let solution_stack = optional_query_param(req, "SolutionStackName");
        let platform_arn = optional_query_param(req, "PlatformArn");
        let inner = format!(
            "{}{}<Options>{}</Options>",
            opt_el("SolutionStackName", &solution_stack),
            opt_el("PlatformArn", &platform_arn),
            configuration_option_descriptions(),
        );
        Ok(self.ok("DescribeConfigurationOptions", inner, req))
    }

    fn validate_configuration_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        required_query_param(req, "ApplicationName")?;
        // All well-formed settings validate clean; no messages.
        let inner = "<Messages/>".to_string();
        Ok(self.ok("ValidateConfigurationSettings", inner, req))
    }
}

// ---------------------------------------------------------------------------
// Events, platform, DNS, tags, account, storage
// ---------------------------------------------------------------------------

impl ElasticBeanstalkService {
    fn describe_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let app_filter = optional_query_param(req, "ApplicationName");
        let env_id_filter = optional_query_param(req, "EnvironmentId");
        let env_name_filter = optional_query_param(req, "EnvironmentName");
        let severity = optional_query_param(req, "Severity");
        let guard = self.state.read();
        let events: Vec<String> = match guard.accounts.get(&req.account_id) {
            Some(acct) => {
                let env_name_from_id = env_id_filter
                    .as_ref()
                    .and_then(|id| acct.environments.get(id).map(|e| e.name.clone()));
                acct.events
                    .iter()
                    .filter(|e| {
                        app_filter
                            .as_ref()
                            .map(|a| e.application_name.as_deref() == Some(a))
                            .unwrap_or(true)
                    })
                    .filter(|e| {
                        let want = env_name_filter.as_deref().or(env_name_from_id.as_deref());
                        want.map(|n| e.environment_name.as_deref() == Some(n))
                            .unwrap_or(true)
                    })
                    .filter(|e| severity.as_ref().map(|s| &e.severity == s).unwrap_or(true))
                    .map(render_event)
                    .collect()
            }
            None => Vec::new(),
        };
        let inner = format!("<Events>{}</Events>", wrap_members(&events));
        Ok(self.ok("DescribeEvents", inner, req))
    }

    fn check_dns_availability(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let prefix = required_query_param(req, "CNAMEPrefix")?;
        check_len("CNAMEPrefix", &prefix, 4, 63)?;
        let fqdn = format!("{prefix}.{}.elasticbeanstalk.com", req.region);
        let guard = self.state.read();
        let taken = guard
            .accounts
            .get(&req.account_id)
            .map(|acct| {
                acct.environments.values().any(|e| {
                    e.cname.starts_with(&format!("{prefix}.")) && e.status != est::TERMINATED
                })
            })
            .unwrap_or(false);
        let inner = format!(
            "{}{}",
            el("Available", if taken { "false" } else { "true" }),
            el("FullyQualifiedCNAME", &fqdn),
        );
        Ok(self.ok("CheckDNSAvailability", inner, req))
    }

    fn list_available_solution_stacks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let stacks: String = SOLUTION_STACKS.iter().map(|s| el("member", s)).collect();
        let details: String = SOLUTION_STACKS
            .iter()
            .map(|s| {
                format!(
                    "<member>{}<PermittedFileTypes><member>zip</member><member>war</member></PermittedFileTypes></member>",
                    el("SolutionStackName", s)
                )
            })
            .collect();
        let inner = format!(
            "<SolutionStacks>{stacks}</SolutionStacks><SolutionStackDetails>{details}</SolutionStackDetails>"
        );
        Ok(self.ok("ListAvailableSolutionStacks", inner, req))
    }

    fn list_platform_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let items: String = SOLUTION_STACKS
            .iter()
            .map(|s| render_platform_summary(s, &req.region))
            .collect();
        let inner = format!("<PlatformSummaryList>{}</PlatformSummaryList>", items);
        Ok(self.ok("ListPlatformVersions", inner, req))
    }

    fn list_platform_branches(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let items: String = SOLUTION_STACKS
            .iter()
            .map(|s| {
                format!(
                    "<member>{}{}{}<SupportedTierList><member>WebServer/Standard</member><member>Worker/SQS/HTTP</member></SupportedTierList></member>",
                    el("PlatformName", platform_family(s)),
                    el("BranchName", s),
                    el("LifecycleState", "Supported"),
                )
            })
            .collect();
        let inner = format!(
            "<PlatformBranchSummaryList>{}</PlatformBranchSummaryList>",
            items
        );
        Ok(self.ok("ListPlatformBranches", inner, req))
    }

    fn describe_platform_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "PlatformArn")?;
        let inner = format!(
            "<PlatformDescription>{}{}{}{}{}</PlatformDescription>",
            el("PlatformArn", &arn),
            el("PlatformOwner", "AWSElasticBeanstalk"),
            el("PlatformStatus", "Ready"),
            el("PlatformCategory", "generic"),
            el("PlatformLifecycleState", "Supported"),
        );
        Ok(self.ok("DescribePlatformVersion", inner, req))
    }

    fn create_platform_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_query_param(req, "PlatformName")?;
        let version = required_query_param(req, "PlatformVersion")?;
        required_query_param(req, "PlatformDefinitionBundle.S3Bucket")
            .or_else(|_| required_query_param(req, "PlatformDefinitionBundle.S3Key"))
            .map_err(|_| missing_parameter("PlatformDefinitionBundle"))?;
        let arn = format!(
            "arn:aws:elasticbeanstalk:{}:{}:platform/{name}/{version}",
            req.region, req.account_id
        );
        let inner = format!(
            "<PlatformSummary>{}{}{}</PlatformSummary>{}",
            el("PlatformArn", &arn),
            el("PlatformOwner", &req.account_id),
            el("PlatformStatus", "Creating"),
            "<Builder/>",
        );
        Ok(self.ok("CreatePlatformVersion", inner, req))
    }

    fn delete_platform_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "PlatformArn")?;
        let inner = format!(
            "<PlatformSummary>{}{}</PlatformSummary>",
            el("PlatformArn", &arn),
            el("PlatformStatus", "Deleting"),
        );
        Ok(self.ok("DeletePlatformVersion", inner, req))
    }

    fn describe_account_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let inner = format!(
            "<ResourceQuotas>{}{}{}{}{}</ResourceQuotas>",
            quota("ApplicationQuota", 75),
            quota("ApplicationVersionQuota", 1000),
            quota("EnvironmentQuota", 200),
            quota("ConfigurationTemplateQuota", 100),
            quota("CustomPlatformQuota", 50),
        );
        Ok(self.ok("DescribeAccountAttributes", inner, req))
    }

    async fn create_storage_location(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let bucket = storage_bucket_name(&req.region, &req.account_id);
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            acct.storage_bucket = Some(bucket.clone());
        }
        let resp = self.ok("CreateStorageLocation", el("S3Bucket", &bucket), req);
        self.save_snapshot().await;
        Ok(resp)
    }

    fn describe_environment_managed_actions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = self.resolve_environment_id(req)?;
        Ok(self.ok(
            "DescribeEnvironmentManagedActions",
            "<ManagedActions/>".to_string(),
            req,
        ))
    }

    fn describe_environment_managed_action_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _ = self.resolve_environment_id(req)?;
        Ok(self.ok(
            "DescribeEnvironmentManagedActionHistory",
            "<ManagedActionHistoryItems/>".to_string(),
            req,
        ))
    }

    fn apply_environment_managed_action(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let action_id = required_query_param(req, "ActionId")?;
        let inner = format!(
            "{}{}{}{}",
            el("ActionId", &action_id),
            el("ActionDescription", "Managed action"),
            el("ActionType", "PlatformUpdate"),
            el("Status", "Scheduled"),
        );
        Ok(self.ok("ApplyEnvironmentManagedAction", inner, req))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ResourceArn")?;
        let guard = self.state.read();
        let tags = guard
            .accounts
            .get(&req.account_id)
            .and_then(|acct| acct.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        let rendered: String = tags
            .iter()
            .map(|t| {
                format!(
                    "<member>{}{}</member>",
                    el("Key", &t.key),
                    el("Value", &t.value)
                )
            })
            .collect();
        let inner = format!(
            "{}<ResourceTags>{}</ResourceTags>",
            el("ResourceArn", &arn),
            rendered
        );
        Ok(self.ok("ListTagsForResource", inner, req))
    }

    async fn update_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_query_param(req, "ResourceArn")?;
        let to_add = parse_tags(req, "TagsToAdd");
        let to_remove = member_list(req, "TagsToRemove");
        {
            let mut guard = self.state.write();
            let acct = guard.get_or_create(&req.account_id);
            let entry = acct.tags.entry(arn).or_default();
            for tag in to_add {
                if let Some(existing) = entry.iter_mut().find(|t| t.key == tag.key) {
                    existing.value = tag.value;
                } else {
                    entry.push(tag);
                }
            }
            entry.retain(|t| !to_remove.contains(&t.key));
        }
        let resp = self.ok_empty("UpdateTagsForResource", req);
        self.save_snapshot().await;
        Ok(resp)
    }

    fn ok_empty(&self, action: &str, req: &AwsRequest) -> AwsResponse {
        AwsResponse::xml(
            StatusCode::OK,
            fakecloud_core::query::query_metadata_only_xml(action, NS, &req.request_id),
        )
    }
}

// ---------------------------------------------------------------------------
// Rendering helpers
// ---------------------------------------------------------------------------

fn wrap_members(items: &[String]) -> String {
    items
        .iter()
        .map(|i| format!("<member>{i}</member>"))
        .collect()
}

fn render_application(app: &Application) -> String {
    format!(
        "{}{}{}{}{}{}",
        el("ApplicationArn", &app.arn),
        el("ApplicationName", &app.name),
        opt_el("Description", &app.description),
        el("DateCreated", &iso(app.date_created)),
        el("DateUpdated", &iso(app.date_updated)),
        render_resource_lifecycle_config(&app.resource_lifecycle_config),
    )
}

fn render_application_with_children(app: &Application, acct: &AccountState) -> String {
    let versions: String = acct
        .versions
        .values()
        .filter(|v| v.application_name == app.name)
        .map(|v| el("member", &v.version_label))
        .collect();
    let templates: String = acct
        .templates
        .values()
        .filter(|t| t.application_name == app.name)
        .map(|t| el("member", &t.template_name))
        .collect();
    format!(
        "{}<Versions>{}</Versions><ConfigurationTemplates>{}</ConfigurationTemplates>",
        render_application(app),
        versions,
        templates,
    )
}

fn render_resource_lifecycle_config(cfg: &ResourceLifecycleConfig) -> String {
    let mut version_cfg = String::new();
    if cfg.max_count_rule.is_some() || cfg.max_age_rule.is_some() {
        version_cfg.push_str("<VersionLifecycleConfig>");
        if let Some(r) = &cfg.max_count_rule {
            version_cfg.push_str(&format!(
                "<MaxCountRule>{}{}{}</MaxCountRule>",
                el("Enabled", if r.enabled { "true" } else { "false" }),
                r.max_count
                    .map(|c| el("MaxCount", &c.to_string()))
                    .unwrap_or_default(),
                r.delete_source_from_s3
                    .map(|d| el("DeleteSourceFromS3", if d { "true" } else { "false" }))
                    .unwrap_or_default(),
            ));
        }
        if let Some(r) = &cfg.max_age_rule {
            version_cfg.push_str(&format!(
                "<MaxAgeRule>{}{}{}</MaxAgeRule>",
                el("Enabled", if r.enabled { "true" } else { "false" }),
                r.max_age_in_days
                    .map(|c| el("MaxAgeInDays", &c.to_string()))
                    .unwrap_or_default(),
                r.delete_source_from_s3
                    .map(|d| el("DeleteSourceFromS3", if d { "true" } else { "false" }))
                    .unwrap_or_default(),
            ));
        }
        version_cfg.push_str("</VersionLifecycleConfig>");
    }
    format!(
        "<ResourceLifecycleConfig>{}{}</ResourceLifecycleConfig>",
        opt_el("ServiceRole", &cfg.service_role),
        version_cfg,
    )
}

fn render_application_version(v: &ApplicationVersion) -> String {
    let source_bundle = match (&v.source_bundle_bucket, &v.source_bundle_key) {
        (Some(b), Some(k)) => format!(
            "<SourceBundle>{}{}</SourceBundle>",
            el("S3Bucket", b),
            el("S3Key", k)
        ),
        _ => String::new(),
    };
    let source_build = v
        .source_build_information
        .as_ref()
        .map(|s| {
            format!(
                "<SourceBuildInformation>{}{}{}</SourceBuildInformation>",
                el("SourceType", &s.source_type),
                el("SourceRepository", &s.source_repository),
                el("SourceLocation", &s.source_location),
            )
        })
        .unwrap_or_default();
    format!(
        "{}{}{}{}{}{}{}{}",
        el("ApplicationVersionArn", &v.arn),
        el("ApplicationName", &v.application_name),
        opt_el("Description", &v.description),
        el("VersionLabel", &v.version_label),
        source_build,
        source_bundle,
        el("DateCreated", &iso(v.date_created)),
        el("DateUpdated", &iso(v.date_updated)) + &el("Status", &v.status),
    )
}

fn render_environment(env: &Environment) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        el("EnvironmentName", &env.name),
        el("EnvironmentId", &env.id),
        el("ApplicationName", &env.application_name),
        opt_el("VersionLabel", &env.version_label),
        opt_el("SolutionStackName", &env.solution_stack_name),
        opt_el("PlatformArn", &env.platform_arn),
        opt_el("TemplateName", &env.template_name),
        opt_el("Description", &env.description),
        el("EndpointURL", &env.endpoint_url),
        el("CNAME", &env.cname),
        el("DateCreated", &iso(env.date_created)),
        el("DateUpdated", &iso(env.date_updated)),
        el("Status", &env.status),
        el(
            "AbortableOperationInProgress",
            if env.abortable_operation_in_progress {
                "true"
            } else {
                "false"
            }
        ),
        el("Health", &env.health) + &el("HealthStatus", &env.health_status),
        render_environment_tier(env),
        el("EnvironmentArn", &env.arn),
        opt_el("OperationsRole", &env.operations_role),
    )
}

fn render_environment_tier(env: &Environment) -> String {
    format!(
        "<Tier>{}{}{}</Tier>",
        el("Name", &env.tier_name),
        el("Type", &env.tier_type),
        el("Version", &env.tier_version),
    )
}

fn render_option_settings(settings: &[OptionSetting]) -> String {
    let members: String = settings
        .iter()
        .map(|s| {
            format!(
                "<member>{}{}{}{}</member>",
                s.resource_name
                    .as_ref()
                    .map(|r| el("ResourceName", r))
                    .unwrap_or_default(),
                el("Namespace", &s.namespace),
                el("OptionName", &s.option_name),
                s.value.as_ref().map(|v| el("Value", v)).unwrap_or_default(),
            )
        })
        .collect();
    format!("<OptionSettings>{members}</OptionSettings>")
}

fn render_configuration_settings(t: &ConfigurationTemplate) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}",
        opt_el("SolutionStackName", &t.solution_stack_name),
        opt_el("PlatformArn", &t.platform_arn),
        el("ApplicationName", &t.application_name),
        el("TemplateName", &t.template_name),
        opt_el("Description", &t.description),
        opt_el("EnvironmentName", &t.environment_name),
        el("DeploymentStatus", &t.deployment_status) + &el("DateCreated", &iso(t.date_created)),
        el("DateUpdated", &iso(t.date_updated)),
        render_option_settings(&t.option_settings),
    )
}

fn render_environment_settings(env: &Environment) -> String {
    format!(
        "{}{}{}{}{}{}{}",
        opt_el("SolutionStackName", &env.solution_stack_name),
        opt_el("PlatformArn", &env.platform_arn),
        el("ApplicationName", &env.application_name),
        el("EnvironmentName", &env.name),
        el("DeploymentStatus", "deployed"),
        el("DateCreated", &iso(env.date_created)) + &el("DateUpdated", &iso(env.date_updated)),
        render_option_settings(&env.option_settings),
    )
}

fn render_event(e: &Event) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}",
        el("EventDate", &iso(e.event_date)),
        el("Message", &e.message),
        opt_el("ApplicationName", &e.application_name),
        opt_el("VersionLabel", &e.version_label),
        opt_el("TemplateName", &e.template_name),
        opt_el("EnvironmentName", &e.environment_name),
        opt_el("PlatformArn", &e.platform_arn),
        opt_el("RequestId", &e.request_id),
        el("Severity", &e.severity),
    )
}

fn render_platform_summary(stack: &str, region: &str) -> String {
    let arn = format!("arn:aws:elasticbeanstalk:{region}::platform/{stack}");
    format!(
        "<member>{}{}{}{}<SupportedTierList><member>WebServer/Standard</member><member>Worker/SQS/HTTP</member></SupportedTierList></member>",
        el("PlatformArn", &arn),
        el("PlatformOwner", "AWSElasticBeanstalk"),
        el("PlatformStatus", "Ready"),
        el("PlatformCategory", platform_family(stack)),
    )
}

fn platform_family(stack: &str) -> &'static str {
    let lower = stack.to_ascii_lowercase();
    if lower.contains("node.js") {
        "Node.js"
    } else if lower.contains("python") {
        "Python"
    } else if lower.contains("corretto") || lower.contains("tomcat") {
        "Java"
    } else if lower.contains("php") {
        "PHP"
    } else if lower.contains("ruby") {
        "Ruby"
    } else if lower.contains("go ") {
        "Go"
    } else if lower.contains(".net") {
        ".NET"
    } else if lower.contains("docker") {
        "Docker"
    } else {
        "generic"
    }
}

fn quota(name: &str, maximum: i64) -> String {
    format!("<{name}><Maximum>{maximum}</Maximum></{name}>")
}

/// A representative set of Elastic Beanstalk configuration options across the
/// most common namespaces.
fn configuration_option_descriptions() -> String {
    let opts: &[(&str, &str, &str, &str)] = &[
        (
            "aws:autoscaling:launchconfiguration",
            "InstanceType",
            "Scalar",
            "t3.micro",
        ),
        ("aws:autoscaling:asg", "MinSize", "Scalar", "1"),
        ("aws:autoscaling:asg", "MaxSize", "Scalar", "4"),
        (
            "aws:elasticbeanstalk:environment",
            "EnvironmentType",
            "Scalar",
            "LoadBalanced",
        ),
        (
            "aws:elasticbeanstalk:environment",
            "ServiceRole",
            "Scalar",
            "aws-elasticbeanstalk-service-role",
        ),
        (
            "aws:elasticbeanstalk:application:environment",
            "PORT",
            "Scalar",
            "",
        ),
        (
            "aws:elasticbeanstalk:healthreporting:system",
            "SystemType",
            "Scalar",
            "enhanced",
        ),
    ];
    opts.iter()
        .map(|(ns, name, value_type, default)| {
            format!(
                "<member>{}{}{}{}{}{}</member>",
                el("Namespace", ns),
                el("Name", name),
                el("DefaultValue", default),
                el("ChangeSeverity", "RestartEnvironment"),
                el("UserDefined", "true"),
                el("ValueType", value_type),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Parsing helpers for nested inputs
// ---------------------------------------------------------------------------

fn parse_tier(req: &AwsRequest) -> (String, String, String) {
    let name = optional_query_param(req, "Tier.Name").unwrap_or_else(|| "WebServer".to_string());
    let type_ = optional_query_param(req, "Tier.Type").unwrap_or_else(|| "Standard".to_string());
    let version = optional_query_param(req, "Tier.Version").unwrap_or_else(|| "1.0".to_string());
    (name, type_, version)
}

fn parse_source_build_information(req: &AwsRequest) -> Option<SourceBuildInformation> {
    let source_type = optional_query_param(req, "SourceBuildInformation.SourceType")?;
    let source_repository = optional_query_param(req, "SourceBuildInformation.SourceRepository")?;
    let source_location = optional_query_param(req, "SourceBuildInformation.SourceLocation")?;
    Some(SourceBuildInformation {
        source_type,
        source_repository,
        source_location,
    })
}

fn parse_resource_lifecycle_config(
    req: &AwsRequest,
    prefix: &str,
) -> Option<ResourceLifecycleConfig> {
    let service_role = optional_query_param(req, &format!("{prefix}.ServiceRole"));
    let max_count = parse_max_count_rule(req, prefix);
    let max_age = parse_max_age_rule(req, prefix);
    if service_role.is_none() && max_count.is_none() && max_age.is_none() {
        return None;
    }
    Some(ResourceLifecycleConfig {
        service_role,
        max_count_rule: max_count,
        max_age_rule: max_age,
    })
}

fn parse_max_count_rule(req: &AwsRequest, prefix: &str) -> Option<MaxCountRule> {
    let base = format!("{prefix}.VersionLifecycleConfig.MaxCountRule");
    let enabled = optional_query_param(req, &format!("{base}.Enabled"))?;
    Some(MaxCountRule {
        enabled: enabled == "true",
        max_count: optional_query_param(req, &format!("{base}.MaxCount"))
            .and_then(|v| v.parse().ok()),
        delete_source_from_s3: optional_query_param(req, &format!("{base}.DeleteSourceFromS3"))
            .map(|v| v == "true"),
    })
}

fn parse_max_age_rule(req: &AwsRequest, prefix: &str) -> Option<MaxAgeRule> {
    let base = format!("{prefix}.VersionLifecycleConfig.MaxAgeRule");
    let enabled = optional_query_param(req, &format!("{base}.Enabled"))?;
    Some(MaxAgeRule {
        enabled: enabled == "true",
        max_age_in_days: optional_query_param(req, &format!("{base}.MaxAgeInDays"))
            .and_then(|v| v.parse().ok()),
        delete_source_from_s3: optional_query_param(req, &format!("{base}.DeleteSourceFromS3"))
            .map(|v| v == "true"),
    })
}

/// Parse `OptionsToRemove.member.N.{Namespace,OptionName,ResourceName}`.
fn parse_options_to_remove(req: &AwsRequest) -> Vec<(Option<String>, String, String)> {
    let mut out = Vec::new();
    for n in 1..=500 {
        let ns = req
            .query_params
            .get(&format!("OptionsToRemove.member.{n}.Namespace"))
            .or_else(|| {
                req.query_params
                    .get(&format!("OptionsToRemove.{n}.Namespace"))
            });
        let name = req
            .query_params
            .get(&format!("OptionsToRemove.member.{n}.OptionName"))
            .or_else(|| {
                req.query_params
                    .get(&format!("OptionsToRemove.{n}.OptionName"))
            });
        let (Some(ns), Some(name)) = (ns, name) else {
            break;
        };
        let resource_name = req
            .query_params
            .get(&format!("OptionsToRemove.member.{n}.ResourceName"))
            .or_else(|| {
                req.query_params
                    .get(&format!("OptionsToRemove.{n}.ResourceName"))
            })
            .cloned();
        out.push((resource_name, ns.clone(), name.clone()));
    }
    out
}

/// Merge new option settings into `current`, then drop any listed in `remove`.
fn apply_option_changes(
    current: &mut Vec<OptionSetting>,
    new_settings: &[OptionSetting],
    remove: &[(Option<String>, String, String)],
) {
    for s in new_settings {
        if let Some(existing) = current.iter_mut().find(|c| {
            c.namespace == s.namespace
                && c.option_name == s.option_name
                && c.resource_name == s.resource_name
        }) {
            existing.value = s.value.clone();
        } else {
            current.push(s.clone());
        }
    }
    current.retain(|c| {
        !remove.iter().any(|(rn, ns, on)| {
            &c.namespace == ns && &c.option_name == on && &c.resource_name == rn
        })
    });
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

#[async_trait]
impl AwsService for ElasticBeanstalkService {
    fn service_name(&self) -> &str {
        "elasticbeanstalk"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_params(&req)?;
        let mutating = matches!(
            req.action.as_str(),
            "CreateApplication"
                | "UpdateApplication"
                | "UpdateApplicationResourceLifecycle"
                | "DeleteApplication"
                | "CreateApplicationVersion"
                | "UpdateApplicationVersion"
                | "DeleteApplicationVersion"
                | "CreateConfigurationTemplate"
                | "UpdateConfigurationTemplate"
                | "DeleteConfigurationTemplate"
                | "AssociateEnvironmentOperationsRole"
                | "DisassociateEnvironmentOperationsRole"
                | "SwapEnvironmentCNAMEs"
        );
        let result = match req.action.as_str() {
            // Applications
            "CreateApplication" => self.create_application(&req),
            "UpdateApplication" => self.update_application(&req),
            "UpdateApplicationResourceLifecycle" => {
                self.update_application_resource_lifecycle(&req)
            }
            "DescribeApplications" => self.describe_applications(&req),
            "DeleteApplication" => self.delete_application(&req),
            // Application versions
            "CreateApplicationVersion" => self.create_application_version(&req),
            "UpdateApplicationVersion" => self.update_application_version(&req),
            "DescribeApplicationVersions" => self.describe_application_versions(&req),
            "DeleteApplicationVersion" => self.delete_application_version(&req),
            // Environments
            "CreateEnvironment" => self.create_environment(&req).await,
            "UpdateEnvironment" => self.update_environment(&req).await,
            "TerminateEnvironment" => self.terminate_environment(&req).await,
            "DescribeEnvironments" => self.describe_environments(&req),
            "DescribeEnvironmentResources" => self.describe_environment_resources(&req),
            "DescribeEnvironmentHealth" => self.describe_environment_health(&req),
            "DescribeInstancesHealth" => self.describe_instances_health(&req),
            "AbortEnvironmentUpdate" => self.abort_environment_update(&req).await,
            "RebuildEnvironment" => {
                self.environment_action(
                    &req,
                    "RebuildEnvironment",
                    "rebuildEnvironment is starting",
                    true,
                )
                .await
            }
            "RestartAppServer" => {
                self.environment_action(
                    &req,
                    "RestartAppServer",
                    "restartAppServer is starting",
                    false,
                )
                .await
            }
            "AssociateEnvironmentOperationsRole" => self.associate_operations_role(&req),
            "DisassociateEnvironmentOperationsRole" => self.disassociate_operations_role(&req),
            "SwapEnvironmentCNAMEs" => self.swap_environment_cnames(&req),
            "ComposeEnvironments" => self.compose_environments(&req),
            "RequestEnvironmentInfo" => self.request_environment_info(&req),
            "RetrieveEnvironmentInfo" => self.retrieve_environment_info(&req),
            "DeleteEnvironmentConfiguration" => self.delete_environment_configuration(&req),
            "DescribeEnvironmentManagedActions" => self.describe_environment_managed_actions(&req),
            "DescribeEnvironmentManagedActionHistory" => {
                self.describe_environment_managed_action_history(&req)
            }
            "ApplyEnvironmentManagedAction" => self.apply_environment_managed_action(&req),
            // Configuration
            "CreateConfigurationTemplate" => self.create_configuration_template(&req),
            "UpdateConfigurationTemplate" => self.update_configuration_template(&req),
            "DeleteConfigurationTemplate" => self.delete_configuration_template(&req),
            "DescribeConfigurationSettings" => self.describe_configuration_settings(&req),
            "DescribeConfigurationOptions" => self.describe_configuration_options(&req),
            "ValidateConfigurationSettings" => self.validate_configuration_settings(&req),
            // Events
            "DescribeEvents" => self.describe_events(&req),
            // DNS
            "CheckDNSAvailability" => self.check_dns_availability(&req),
            // Platforms / solution stacks
            "ListAvailableSolutionStacks" => self.list_available_solution_stacks(&req),
            "ListPlatformVersions" => self.list_platform_versions(&req),
            "ListPlatformBranches" => self.list_platform_branches(&req),
            "DescribePlatformVersion" => self.describe_platform_version(&req),
            "CreatePlatformVersion" => self.create_platform_version(&req),
            "DeletePlatformVersion" => self.delete_platform_version(&req),
            // Account / storage
            "DescribeAccountAttributes" => self.describe_account_attributes(&req),
            "CreateStorageLocation" => self.create_storage_location(&req).await,
            // Tags
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "UpdateTagsForResource" => self.update_tags_for_resource(&req).await,
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidAction",
                format!("Action {other} is not supported"),
            )),
        };
        if mutating && result.is_ok() {
            self.save_snapshot().await;
        }
        result
    }
}

#[cfg(test)]
mod tests;
