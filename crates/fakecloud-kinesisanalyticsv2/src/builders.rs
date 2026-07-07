//! Shared record builders used by BOTH the direct API handlers (`service.rs`)
//! and the CloudFormation `AWS::KinesisAnalyticsV2::*` provisioner, so a
//! CFN-created application / output / reference-data-source / CloudWatch-logging
//! option is byte-for-byte identical to its direct-API equivalent and the two
//! paths cannot diverge (#1766). Each builder mutates the shared [`Ka2State`] in
//! place exactly as the direct `CreateApplication` / `AddApplication*` handler
//! does (inserting the same `ApplicationConfigurationDescription` /
//! `CloudWatchLoggingOptionDescription` wire objects), so a CFN-provisioned
//! resource reads back identically on `DescribeApplication` AND survives a
//! restart through the `kinesisanalyticsv2` snapshot hook.

use std::collections::BTreeMap;

use chrono::Utc;
use serde_json::Value;

use crate::service::{
    arn, bump_version, cloudwatch_desc, config_to_description, new_token, output_desc,
    record_version, reference_desc, sql_array_push, sql_array_retain, DEFAULT_MAINTENANCE_END,
    DEFAULT_MAINTENANCE_START,
};
use crate::state::{Application, Ka2State};

/// Build + insert an `Application` into `st` exactly as the direct
/// `CreateApplication` handler does, returning the application ARN. Shared with
/// the `AWS::KinesisAnalyticsV2::Application` provisioner. Returns `Err` when an
/// application of that name already exists (mapped to `ResourceInUseException`
/// on the direct path). `application_configuration` / `cloudwatch_logging_options`
/// are the raw AWS input-shaped `ApplicationConfiguration` object and
/// `CloudWatchLoggingOptions` array (PascalCase, identical between the direct API
/// and CloudFormation), or `None`.
#[allow(clippy::too_many_arguments)]
pub fn insert_application(
    st: &mut Ka2State,
    region: &str,
    account: &str,
    name: &str,
    description: Option<String>,
    runtime: &str,
    role: &str,
    mode: Option<String>,
    application_configuration: Option<&Value>,
    cloudwatch_logging_options: Option<&Value>,
    tags: BTreeMap<String, String>,
) -> Result<String, String> {
    if st.applications.contains_key(name) {
        return Err(format!("Application {name} already exists."));
    }
    let now = Utc::now();
    let application_arn = arn(region, account, name);
    let mut id_counter = 0u64;
    let config_description = match application_configuration {
        Some(cfg) if cfg.is_object() => config_to_description(cfg, Some(role), &mut id_counter),
        _ => Value::Object(serde_json::Map::new()),
    };
    let cloudwatch: Vec<Value> = cloudwatch_logging_options
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|o| {
                    id_counter += 1;
                    cloudwatch_desc(o, &id_counter.to_string(), Some(role))
                })
                .collect()
        })
        .unwrap_or_default();

    let mut app = Application {
        name: name.to_string(),
        arn: application_arn.clone(),
        description,
        runtime_environment: runtime.to_string(),
        service_execution_role: Some(role.to_string()),
        application_mode: mode,
        status: "READY".to_string(),
        version_id: 1,
        create_timestamp: now,
        last_update_timestamp: now,
        conditional_token: new_token(),
        config_description,
        cloudwatch_logging_options: cloudwatch,
        maintenance_start: DEFAULT_MAINTENANCE_START.to_string(),
        maintenance_end: DEFAULT_MAINTENANCE_END.to_string(),
        snapshots: BTreeMap::new(),
        operations: Vec::new(),
        versions: BTreeMap::new(),
        id_counter,
        version_updated_from: None,
        version_rolled_back_from: None,
        version_rolled_back_to: None,
        flink_binding: None,
    };
    record_version(&mut app);
    if !tags.is_empty() {
        st.tags.insert(application_arn.clone(), tags);
    }
    st.applications.insert(name.to_string(), app);
    Ok(application_arn)
}

/// Append an `Output` to a SQL application exactly as `AddApplicationOutput`
/// does, returning the minted `OutputId`. Shared with the
/// `AWS::KinesisAnalyticsV2::ApplicationOutput` provisioner (whose `Ref` is the
/// output id).
pub fn insert_output(st: &mut Ka2State, name: &str, output: &Value) -> Result<String, String> {
    let app = st
        .applications
        .get_mut(name)
        .ok_or_else(|| format!("Application {name} not found."))?;
    let id = app.next_id();
    let role = app.service_execution_role.clone();
    let desc = output_desc(output, &mut || id.clone(), role.as_deref());
    sql_array_push(&mut app.config_description, "OutputDescriptions", desc);
    bump_version(app);
    Ok(id)
}

/// Append a `ReferenceDataSource` exactly as `AddApplicationReferenceDataSource`
/// does, returning the minted `ReferenceId`. Shared with the
/// `AWS::KinesisAnalyticsV2::ApplicationReferenceDataSource` provisioner (whose
/// `Ref` is the reference id).
pub fn insert_reference_data_source(
    st: &mut Ka2State,
    name: &str,
    rds: &Value,
) -> Result<String, String> {
    let app = st
        .applications
        .get_mut(name)
        .ok_or_else(|| format!("Application {name} not found."))?;
    let id = app.next_id();
    let role = app.service_execution_role.clone();
    let desc = reference_desc(rds, &mut || id.clone(), role.as_deref());
    sql_array_push(
        &mut app.config_description,
        "ReferenceDataSourceDescriptions",
        desc,
    );
    bump_version(app);
    Ok(id)
}

/// Append a `CloudWatchLoggingOption` exactly as
/// `AddApplicationCloudWatchLoggingOption` does, returning the minted
/// `CloudWatchLoggingOptionId`. Shared with the
/// `AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption` provisioner
/// (whose `Ref` is the logging-option id).
pub fn insert_cloudwatch_logging_option(
    st: &mut Ka2State,
    name: &str,
    opt: &Value,
) -> Result<String, String> {
    let app = st
        .applications
        .get_mut(name)
        .ok_or_else(|| format!("Application {name} not found."))?;
    let id = app.next_id();
    let role = app.service_execution_role.clone();
    app.cloudwatch_logging_options
        .push(cloudwatch_desc(opt, &id, role.as_deref()));
    bump_version(app);
    Ok(id)
}

/// Remove an application output by id, mirroring `DeleteApplicationOutput`. A
/// no-op when the application or output is absent (idempotent teardown).
pub fn remove_output(st: &mut Ka2State, name: &str, id: &str) {
    if let Some(app) = st.applications.get_mut(name) {
        sql_array_retain(
            &mut app.config_description,
            "OutputDescriptions",
            "OutputId",
            id,
        );
        bump_version(app);
    }
}

/// Remove a reference data source by id, mirroring
/// `DeleteApplicationReferenceDataSource`. Idempotent.
pub fn remove_reference_data_source(st: &mut Ka2State, name: &str, id: &str) {
    if let Some(app) = st.applications.get_mut(name) {
        sql_array_retain(
            &mut app.config_description,
            "ReferenceDataSourceDescriptions",
            "ReferenceId",
            id,
        );
        bump_version(app);
    }
}

/// Remove a CloudWatch logging option by id, mirroring
/// `DeleteApplicationCloudWatchLoggingOption`. Idempotent.
pub fn remove_cloudwatch_logging_option(st: &mut Ka2State, name: &str, id: &str) {
    if let Some(app) = st.applications.get_mut(name) {
        app.cloudwatch_logging_options
            .retain(|o| o.get("CloudWatchLoggingOptionId").and_then(Value::as_str) != Some(id));
        bump_version(app);
    }
}
