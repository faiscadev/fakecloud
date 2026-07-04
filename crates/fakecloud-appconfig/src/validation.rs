//! Model-derived input validation for AWS AppConfig.
//!
//! AWS rejects requests that violate the Smithy model's top-level constraints
//! (`@required`, `@length`, `@range`, `@enum`) with `BadRequestException`
//! before the operation runs. This module encodes those constraints per
//! operation so out-of-range / omitted / bad-enum inputs are rejected exactly
//! as the real service does. The rule table is generated from
//! `aws-models/appconfig.json`.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::{AwsRequest, AwsServiceError};

/// Where a constrained input member is bound on the wire.
#[derive(Clone, Copy)]
pub enum Src {
    Body,
    Query,
    Label,
}

/// A single input-constraint rule from the Smithy model.
pub enum Rule {
    Required(&'static str, Src),
    LenMin(&'static str, Src, usize),
    LenMax(&'static str, Src, usize),
    RangeMin(&'static str, Src, f64),
    RangeMax(&'static str, Src, f64),
    Enum(&'static str, Src, &'static [&'static str]),
}

fn bad(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

/// Length of a value for `@length` checks: strings by char count, lists by
/// element count, maps by entry count.
fn value_len(v: &Value) -> Option<usize> {
    match v {
        Value::String(s) => Some(s.chars().count()),
        Value::Array(a) => Some(a.len()),
        Value::Object(o) => Some(o.len()),
        _ => None,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Validate an operation's input against the model constraints. `labels` are
/// the decoded path labels in URI order (see `label_fields`).
pub fn validate(action: &str, labels: &[String], req: &AwsRequest) -> Result<(), AwsServiceError> {
    let rules = input_rules(action);
    if rules.is_empty() {
        return Ok(());
    }
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
    let lf = label_fields(action);
    let raw = |field: &str, src: Src| -> Option<Value> {
        match src {
            Src::Body => body.get(field).cloned().filter(|v| !v.is_null()),
            Src::Query => req
                .query_params
                .get(field)
                .map(|s| Value::String(s.clone())),
            Src::Label => lf
                .iter()
                .position(|f| *f == field)
                .and_then(|i| labels.get(i))
                .map(|s| Value::String(s.clone())),
        }
    };
    for rule in rules {
        match rule {
            Rule::Required(f, src) => {
                let missing = match raw(f, src) {
                    None => true,
                    // An omitted path label reaches us as the literal
                    // `{Field}` placeholder or an empty segment.
                    Some(Value::String(s)) => {
                        s.is_empty() || (s.starts_with('{') && s.ends_with('}'))
                    }
                    Some(_) => false,
                };
                if missing {
                    return Err(bad(format!("{f} is required.")));
                }
            }
            Rule::LenMin(f, src, n) => {
                if let Some(len) = raw(f, src).as_ref().and_then(value_len) {
                    if len < n {
                        return Err(bad(format!("{f} is shorter than the minimum length.")));
                    }
                }
            }
            Rule::LenMax(f, src, n) => {
                if let Some(len) = raw(f, src).as_ref().and_then(value_len) {
                    if len > n {
                        return Err(bad(format!("{f} exceeds the maximum length.")));
                    }
                }
            }
            Rule::RangeMin(f, src, n) => {
                if let Some(x) = raw(f, src).as_ref().and_then(as_f64) {
                    if x < n {
                        return Err(bad(format!("{f} is below the minimum value.")));
                    }
                }
            }
            Rule::RangeMax(f, src, n) => {
                if let Some(x) = raw(f, src).as_ref().and_then(as_f64) {
                    if x > n {
                        return Err(bad(format!("{f} exceeds the maximum value.")));
                    }
                }
            }
            Rule::Enum(f, src, vals) => {
                if let Some(Value::String(s)) = raw(f, src) {
                    if !vals.contains(&s.as_str()) {
                        return Err(bad(format!("{f} is not a valid value.")));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Path-label field names in URI order for each operation, so `Src::Label`
/// rules can resolve their value from the positional `labels` slice.
fn label_fields(action: &str) -> &'static [&'static str] {
    match action {
        "CreateConfigurationProfile" => &["ApplicationId"],
        "CreateEnvironment" => &["ApplicationId"],
        "CreateExperimentDefinition" => &["ApplicationIdentifier"],
        "CreateHostedConfigurationVersion" => &["ApplicationId", "ConfigurationProfileId"],
        "DeleteApplication" => &["ApplicationId"],
        "DeleteConfigurationProfile" => &["ApplicationId", "ConfigurationProfileId"],
        "DeleteDeploymentStrategy" => &["DeploymentStrategyId"],
        "DeleteEnvironment" => &["ApplicationId", "EnvironmentId"],
        "DeleteExperimentDefinition" => {
            &["ApplicationIdentifier", "ExperimentDefinitionIdentifier"]
        }
        "DeleteExtension" => &["ExtensionIdentifier"],
        "DeleteExtensionAssociation" => &["ExtensionAssociationId"],
        "DeleteHostedConfigurationVersion" => {
            &["ApplicationId", "ConfigurationProfileId", "VersionNumber"]
        }
        "GetApplication" => &["ApplicationId"],
        "GetConfiguration" => &["Application", "Environment", "Configuration"],
        "GetConfigurationProfile" => &["ApplicationId", "ConfigurationProfileId"],
        "GetDeployment" => &["ApplicationId", "EnvironmentId", "DeploymentNumber"],
        "GetDeploymentStrategy" => &["DeploymentStrategyId"],
        "GetEnvironment" => &["ApplicationId", "EnvironmentId"],
        "GetExperimentDefinition" => &["ApplicationIdentifier", "ExperimentDefinitionIdentifier"],
        "GetExperimentRun" => &[
            "ApplicationIdentifier",
            "ExperimentDefinitionIdentifier",
            "Run",
        ],
        "GetExtension" => &["ExtensionIdentifier"],
        "GetExtensionAssociation" => &["ExtensionAssociationId"],
        "GetHostedConfigurationVersion" => {
            &["ApplicationId", "ConfigurationProfileId", "VersionNumber"]
        }
        "ListConfigurationProfiles" => &["ApplicationId"],
        "ListDeployments" => &["ApplicationId", "EnvironmentId"],
        "ListEnvironments" => &["ApplicationId"],
        "ListExperimentRunEvents" => &[
            "ApplicationIdentifier",
            "ExperimentDefinitionIdentifier",
            "Run",
        ],
        "ListExperimentRuns" => &["ApplicationIdentifier", "ExperimentDefinitionIdentifier"],
        "ListHostedConfigurationVersions" => &["ApplicationId", "ConfigurationProfileId"],
        "ListTagsForResource" => &["ResourceArn"],
        "StartDeployment" => &["ApplicationId", "EnvironmentId"],
        "StartExperimentRun" => &["ApplicationIdentifier", "ExperimentDefinitionIdentifier"],
        "StopDeployment" => &["ApplicationId", "EnvironmentId", "DeploymentNumber"],
        "StopExperimentRun" => &[
            "ApplicationIdentifier",
            "ExperimentDefinitionIdentifier",
            "Run",
        ],
        "TagResource" => &["ResourceArn"],
        "UntagResource" => &["ResourceArn"],
        "UpdateApplication" => &["ApplicationId"],
        "UpdateConfigurationProfile" => &["ApplicationId", "ConfigurationProfileId"],
        "UpdateDeploymentStrategy" => &["DeploymentStrategyId"],
        "UpdateEnvironment" => &["ApplicationId", "EnvironmentId"],
        "UpdateExperimentDefinition" => {
            &["ApplicationIdentifier", "ExperimentDefinitionIdentifier"]
        }
        "UpdateExperimentRun" => &[
            "ApplicationIdentifier",
            "ExperimentDefinitionIdentifier",
            "Run",
        ],
        "UpdateExtension" => &["ExtensionIdentifier"],
        "UpdateExtensionAssociation" => &["ExtensionAssociationId"],
        "ValidateConfiguration" => &["ApplicationId", "ConfigurationProfileId"],
        _ => &[],
    }
}

/// Model-derived constraint rules per operation.
#[allow(clippy::too_many_lines)]
fn input_rules(action: &str) -> Vec<Rule> {
    match action {
        "CreateApplication" => vec![
            Rule::Required("Name", Src::Body),
            Rule::LenMin("Name", Src::Body, 1),
            Rule::LenMax("Name", Src::Body, 64),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "CreateConfigurationProfile" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("Name", Src::Body),
            Rule::LenMin("Name", Src::Body, 1),
            Rule::LenMax("Name", Src::Body, 128),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::Required("LocationUri", Src::Body),
            Rule::LenMin("LocationUri", Src::Body, 1),
            Rule::LenMax("LocationUri", Src::Body, 2048),
            Rule::LenMin("RetrievalRoleArn", Src::Body, 20),
            Rule::LenMax("RetrievalRoleArn", Src::Body, 2048),
            Rule::LenMin("Validators", Src::Body, 0),
            Rule::LenMax("Validators", Src::Body, 2),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
            Rule::LenMin("KmsKeyIdentifier", Src::Body, 1),
            Rule::LenMax("KmsKeyIdentifier", Src::Body, 2048),
        ],
        "CreateDeploymentStrategy" => vec![
            Rule::Required("Name", Src::Body),
            Rule::LenMin("Name", Src::Body, 1),
            Rule::LenMax("Name", Src::Body, 64),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::Required("DeploymentDurationInMinutes", Src::Body),
            Rule::RangeMin("DeploymentDurationInMinutes", Src::Body, 0.0),
            Rule::RangeMax("DeploymentDurationInMinutes", Src::Body, 1440.0),
            Rule::RangeMin("FinalBakeTimeInMinutes", Src::Body, 0.0),
            Rule::RangeMax("FinalBakeTimeInMinutes", Src::Body, 1440.0),
            Rule::Required("GrowthFactor", Src::Body),
            Rule::RangeMin("GrowthFactor", Src::Body, 1.0),
            Rule::RangeMax("GrowthFactor", Src::Body, 100.0),
            Rule::Enum("GrowthType", Src::Body, &["LINEAR", "EXPONENTIAL"]),
            Rule::Enum("ReplicateTo", Src::Body, &["NONE", "SSM_DOCUMENT"]),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "CreateEnvironment" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("Name", Src::Body),
            Rule::LenMin("Name", Src::Body, 1),
            Rule::LenMax("Name", Src::Body, 64),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::LenMin("Monitors", Src::Body, 0),
            Rule::LenMax("Monitors", Src::Body, 5),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "CreateExperimentDefinition" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("Name", Src::Body),
            Rule::Required("ConfigurationProfileIdentifier", Src::Body),
            Rule::LenMin("ConfigurationProfileIdentifier", Src::Body, 1),
            Rule::LenMax("ConfigurationProfileIdentifier", Src::Body, 2048),
            Rule::Required("EnvironmentIdentifier", Src::Body),
            Rule::LenMin("EnvironmentIdentifier", Src::Body, 1),
            Rule::LenMax("EnvironmentIdentifier", Src::Body, 2048),
            Rule::Required("FlagKey", Src::Body),
            Rule::Required("Treatments", Src::Body),
            Rule::LenMin("Treatments", Src::Body, 1),
            Rule::LenMax("Treatments", Src::Body, 5),
            Rule::Required("Control", Src::Body),
            Rule::Required("AudienceRule", Src::Body),
            Rule::LenMin("AudienceRule", Src::Body, 1),
            Rule::LenMax("AudienceRule", Src::Body, 16384),
            Rule::LenMin("Hypothesis", Src::Body, 0),
            Rule::LenMax("Hypothesis", Src::Body, 1024),
            Rule::LenMin("AudienceDescription", Src::Body, 0),
            Rule::LenMax("AudienceDescription", Src::Body, 1024),
            Rule::LenMin("LaunchCriteria", Src::Body, 0),
            Rule::LenMax("LaunchCriteria", Src::Body, 1024),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "CreateExtension" => vec![
            Rule::Required("Name", Src::Body),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::Required("Actions", Src::Body),
            Rule::LenMin("Actions", Src::Body, 1),
            Rule::LenMax("Actions", Src::Body, 5),
            Rule::LenMin("Parameters", Src::Body, 1),
            Rule::LenMax("Parameters", Src::Body, 10),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "CreateExtensionAssociation" => vec![
            Rule::Required("ExtensionIdentifier", Src::Body),
            Rule::LenMin("ExtensionIdentifier", Src::Body, 1),
            Rule::LenMax("ExtensionIdentifier", Src::Body, 2048),
            Rule::Required("ResourceIdentifier", Src::Body),
            Rule::LenMin("ResourceIdentifier", Src::Body, 1),
            Rule::LenMax("ResourceIdentifier", Src::Body, 2048),
            Rule::LenMin("Parameters", Src::Body, 0),
            Rule::LenMax("Parameters", Src::Body, 10),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "CreateHostedConfigurationVersion" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
        ],
        "DeleteApplication" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
        ],
        "DeleteConfigurationProfile" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
        ],
        "DeleteDeploymentStrategy" => vec![Rule::Required("DeploymentStrategyId", Src::Label)],
        "DeleteEnvironment" => vec![
            Rule::Required("EnvironmentId", Src::Label),
            Rule::LenMin("EnvironmentId", Src::Label, 1),
            Rule::LenMax("EnvironmentId", Src::Label, 64),
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
        ],
        "DeleteExperimentDefinition" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::Enum("delete_type", Src::Query, &["ARCHIVE", "DESTROY"]),
        ],
        "DeleteExtension" => vec![
            Rule::Required("ExtensionIdentifier", Src::Label),
            Rule::LenMin("ExtensionIdentifier", Src::Label, 1),
            Rule::LenMax("ExtensionIdentifier", Src::Label, 2048),
        ],
        "DeleteExtensionAssociation" => vec![Rule::Required("ExtensionAssociationId", Src::Label)],
        "DeleteHostedConfigurationVersion" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
            Rule::Required("VersionNumber", Src::Label),
        ],
        "GetApplication" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
        ],
        "GetConfiguration" => vec![
            Rule::Required("Application", Src::Label),
            Rule::LenMin("Application", Src::Label, 1),
            Rule::LenMax("Application", Src::Label, 64),
            Rule::Required("Environment", Src::Label),
            Rule::LenMin("Environment", Src::Label, 1),
            Rule::LenMax("Environment", Src::Label, 64),
            Rule::Required("Configuration", Src::Label),
            Rule::LenMin("Configuration", Src::Label, 1),
            Rule::LenMax("Configuration", Src::Label, 64),
            Rule::Required("client_id", Src::Query),
            Rule::LenMin("client_id", Src::Query, 1),
            Rule::LenMax("client_id", Src::Query, 64),
            Rule::LenMin("client_configuration_version", Src::Query, 1),
            Rule::LenMax("client_configuration_version", Src::Query, 1024),
        ],
        "GetConfigurationProfile" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
        ],
        "GetDeployment" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("EnvironmentId", Src::Label),
            Rule::LenMin("EnvironmentId", Src::Label, 1),
            Rule::LenMax("EnvironmentId", Src::Label, 64),
            Rule::Required("DeploymentNumber", Src::Label),
        ],
        "GetDeploymentStrategy" => vec![Rule::Required("DeploymentStrategyId", Src::Label)],
        "GetEnvironment" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("EnvironmentId", Src::Label),
            Rule::LenMin("EnvironmentId", Src::Label, 1),
            Rule::LenMax("EnvironmentId", Src::Label, 64),
        ],
        "GetExperimentDefinition" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
        ],
        "GetExperimentRun" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::Required("Run", Src::Label),
            Rule::RangeMin("Run", Src::Label, 1.0),
        ],
        "GetExtension" => vec![
            Rule::Required("ExtensionIdentifier", Src::Label),
            Rule::LenMin("ExtensionIdentifier", Src::Label, 1),
            Rule::LenMax("ExtensionIdentifier", Src::Label, 2048),
        ],
        "GetExtensionAssociation" => vec![Rule::Required("ExtensionAssociationId", Src::Label)],
        "GetHostedConfigurationVersion" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
            Rule::Required("VersionNumber", Src::Label),
        ],
        "ListApplications" => vec![
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListConfigurationProfiles" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListDeploymentStrategies" => vec![
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListDeployments" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("EnvironmentId", Src::Label),
            Rule::LenMin("EnvironmentId", Src::Label, 1),
            Rule::LenMax("EnvironmentId", Src::Label, 64),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListEnvironments" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListExperimentDefinitions" => vec![
            Rule::LenMin("application_identifier", Src::Query, 1),
            Rule::LenMax("application_identifier", Src::Query, 2048),
            Rule::LenMin("configuration_profile_identifier", Src::Query, 1),
            Rule::LenMax("configuration_profile_identifier", Src::Query, 2048),
            Rule::LenMin("environment_identifier", Src::Query, 1),
            Rule::LenMax("environment_identifier", Src::Query, 2048),
            Rule::Enum("status", Src::Query, &["ACTIVE", "IDLE", "ARCHIVED"]),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListExperimentRunEvents" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::Required("Run", Src::Label),
            Rule::RangeMin("Run", Src::Label, 1.0),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListExperimentRuns" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
            Rule::Enum("status", Src::Query, &["RUNNING", "DONE"]),
        ],
        "ListExtensionAssociations" => vec![
            Rule::LenMin("resource_identifier", Src::Query, 20),
            Rule::LenMax("resource_identifier", Src::Query, 2048),
            Rule::LenMin("extension_identifier", Src::Query, 1),
            Rule::LenMax("extension_identifier", Src::Query, 2048),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
        ],
        "ListExtensions" => vec![
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
            Rule::LenMin("name", Src::Query, 1),
            Rule::LenMax("name", Src::Query, 64),
        ],
        "ListHostedConfigurationVersions" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
            Rule::RangeMin("max_results", Src::Query, 1.0),
            Rule::RangeMax("max_results", Src::Query, 50.0),
            Rule::LenMin("next_token", Src::Query, 1),
            Rule::LenMax("next_token", Src::Query, 2048),
            Rule::LenMin("version_label", Src::Query, 1),
            Rule::LenMax("version_label", Src::Query, 64),
        ],
        "ListTagsForResource" => vec![
            Rule::Required("ResourceArn", Src::Label),
            Rule::LenMin("ResourceArn", Src::Label, 20),
            Rule::LenMax("ResourceArn", Src::Label, 2048),
        ],
        "StartDeployment" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("EnvironmentId", Src::Label),
            Rule::LenMin("EnvironmentId", Src::Label, 1),
            Rule::LenMax("EnvironmentId", Src::Label, 64),
            Rule::Required("DeploymentStrategyId", Src::Body),
            Rule::Required("ConfigurationProfileId", Src::Body),
            Rule::LenMin("ConfigurationProfileId", Src::Body, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Body, 128),
            Rule::Required("ConfigurationVersion", Src::Body),
            Rule::LenMin("ConfigurationVersion", Src::Body, 1),
            Rule::LenMax("ConfigurationVersion", Src::Body, 1024),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
            Rule::LenMin("KmsKeyIdentifier", Src::Body, 1),
            Rule::LenMax("KmsKeyIdentifier", Src::Body, 2048),
            Rule::LenMin("DynamicExtensionParameters", Src::Body, 1),
            Rule::LenMax("DynamicExtensionParameters", Src::Body, 10),
        ],
        "StartExperimentRun" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::RangeMin("ExposurePercentage", Src::Body, 0.0),
            Rule::RangeMax("ExposurePercentage", Src::Body, 100.0),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "StopDeployment" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("EnvironmentId", Src::Label),
            Rule::LenMin("EnvironmentId", Src::Label, 1),
            Rule::LenMax("EnvironmentId", Src::Label, 64),
            Rule::Required("DeploymentNumber", Src::Label),
        ],
        "StopExperimentRun" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::Required("Run", Src::Label),
            Rule::RangeMin("Run", Src::Label, 1.0),
        ],
        "TagResource" => vec![
            Rule::Required("ResourceArn", Src::Label),
            Rule::LenMin("ResourceArn", Src::Label, 20),
            Rule::LenMax("ResourceArn", Src::Label, 2048),
            Rule::Required("Tags", Src::Body),
            Rule::LenMin("Tags", Src::Body, 0),
            Rule::LenMax("Tags", Src::Body, 50),
        ],
        "UntagResource" => vec![
            Rule::Required("ResourceArn", Src::Label),
            Rule::LenMin("ResourceArn", Src::Label, 20),
            Rule::LenMax("ResourceArn", Src::Label, 2048),
            Rule::Required("tagKeys", Src::Query),
            Rule::LenMin("tagKeys", Src::Query, 0),
            Rule::LenMax("tagKeys", Src::Query, 50),
        ],
        "UpdateApplication" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::LenMin("Name", Src::Body, 1),
            Rule::LenMax("Name", Src::Body, 64),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
        ],
        "UpdateConfigurationProfile" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
            Rule::LenMin("Name", Src::Body, 1),
            Rule::LenMax("Name", Src::Body, 128),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::LenMin("RetrievalRoleArn", Src::Body, 20),
            Rule::LenMax("RetrievalRoleArn", Src::Body, 2048),
            Rule::LenMin("Validators", Src::Body, 0),
            Rule::LenMax("Validators", Src::Body, 2),
            Rule::LenMin("KmsKeyIdentifier", Src::Body, 0),
            Rule::LenMax("KmsKeyIdentifier", Src::Body, 2048),
        ],
        "UpdateDeploymentStrategy" => vec![
            Rule::Required("DeploymentStrategyId", Src::Label),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::RangeMin("DeploymentDurationInMinutes", Src::Body, 0.0),
            Rule::RangeMax("DeploymentDurationInMinutes", Src::Body, 1440.0),
            Rule::RangeMin("FinalBakeTimeInMinutes", Src::Body, 0.0),
            Rule::RangeMax("FinalBakeTimeInMinutes", Src::Body, 1440.0),
            Rule::RangeMin("GrowthFactor", Src::Body, 1.0),
            Rule::RangeMax("GrowthFactor", Src::Body, 100.0),
            Rule::Enum("GrowthType", Src::Body, &["LINEAR", "EXPONENTIAL"]),
        ],
        "UpdateEnvironment" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("EnvironmentId", Src::Label),
            Rule::LenMin("EnvironmentId", Src::Label, 1),
            Rule::LenMax("EnvironmentId", Src::Label, 64),
            Rule::LenMin("Name", Src::Body, 1),
            Rule::LenMax("Name", Src::Body, 64),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::LenMin("Monitors", Src::Body, 0),
            Rule::LenMax("Monitors", Src::Body, 5),
        ],
        "UpdateExperimentDefinition" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::LenMin("Treatments", Src::Body, 1),
            Rule::LenMax("Treatments", Src::Body, 5),
            Rule::LenMin("Hypothesis", Src::Body, 0),
            Rule::LenMax("Hypothesis", Src::Body, 1024),
            Rule::LenMin("AudienceRule", Src::Body, 1),
            Rule::LenMax("AudienceRule", Src::Body, 16384),
            Rule::LenMin("AudienceDescription", Src::Body, 0),
            Rule::LenMax("AudienceDescription", Src::Body, 1024),
            Rule::LenMin("LaunchCriteria", Src::Body, 0),
            Rule::LenMax("LaunchCriteria", Src::Body, 1024),
        ],
        "UpdateExperimentRun" => vec![
            Rule::Required("ApplicationIdentifier", Src::Label),
            Rule::LenMin("ApplicationIdentifier", Src::Label, 1),
            Rule::LenMax("ApplicationIdentifier", Src::Label, 2048),
            Rule::Required("ExperimentDefinitionIdentifier", Src::Label),
            Rule::LenMin("ExperimentDefinitionIdentifier", Src::Label, 1),
            Rule::LenMax("ExperimentDefinitionIdentifier", Src::Label, 2048),
            Rule::Required("Run", Src::Label),
            Rule::RangeMin("Run", Src::Label, 1.0),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::RangeMin("ExposurePercentage", Src::Body, 0.0),
            Rule::RangeMax("ExposurePercentage", Src::Body, 100.0),
        ],
        "UpdateExtension" => vec![
            Rule::Required("ExtensionIdentifier", Src::Label),
            Rule::LenMin("ExtensionIdentifier", Src::Label, 1),
            Rule::LenMax("ExtensionIdentifier", Src::Label, 2048),
            Rule::LenMin("Description", Src::Body, 0),
            Rule::LenMax("Description", Src::Body, 1024),
            Rule::LenMin("Actions", Src::Body, 1),
            Rule::LenMax("Actions", Src::Body, 5),
            Rule::LenMin("Parameters", Src::Body, 1),
            Rule::LenMax("Parameters", Src::Body, 10),
        ],
        "UpdateExtensionAssociation" => vec![
            Rule::Required("ExtensionAssociationId", Src::Label),
            Rule::LenMin("Parameters", Src::Body, 0),
            Rule::LenMax("Parameters", Src::Body, 10),
        ],
        "ValidateConfiguration" => vec![
            Rule::Required("ApplicationId", Src::Label),
            Rule::LenMin("ApplicationId", Src::Label, 1),
            Rule::LenMax("ApplicationId", Src::Label, 64),
            Rule::Required("ConfigurationProfileId", Src::Label),
            Rule::LenMin("ConfigurationProfileId", Src::Label, 1),
            Rule::LenMax("ConfigurationProfileId", Src::Label, 128),
            Rule::Required("configuration_version", Src::Query),
            Rule::LenMin("configuration_version", Src::Query, 1),
            Rule::LenMax("configuration_version", Src::Query, 1024),
        ],
        _ => vec![],
    }
}
