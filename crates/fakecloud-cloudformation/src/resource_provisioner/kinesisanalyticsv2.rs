//! `AWS::KinesisAnalyticsV2::*` CloudFormation provisioning (Amazon Managed
//! Service for Apache Flink / Kinesis Data Analytics v2, `kinesisanalyticsv2`).
//!
//! Each resource is written through to the `kinesisanalyticsv2` service state as
//! the same wire JSON the direct `CreateApplication` / `AddApplication*` handlers
//! store -- via the shared `fakecloud_kinesisanalyticsv2::builders` module -- so
//! a CFN-created application (and its outputs / reference data sources /
//! CloudWatch logging options) reads back identically on `DescribeApplication`
//! and persists through the `kinesisanalyticsv2` snapshot hook (survives a
//! restart -- the #1766 phantom-resource lesson). The application is inserted
//! synchronously and settles to `READY` exactly as the direct `CreateApplication`
//! does (which never starts a Flink job -- `StartApplication` is a separate
//! action), so `Ref` resolves during provisioning and CFN never spawns a Flink
//! container.
//!
//! Physical id + `Ref` / `Fn::GetAtt` (verified against the AWS CloudFormation
//! resource specification):
//!   Application                      -> physical id / Ref = ApplicationName;
//!                                        no returnable Fn::GetAtt attributes
//!   ApplicationOutput                -> physical id / Ref = the output Id
//!   ApplicationReferenceDataSource   -> physical id / Ref = the reference Id
//!   ApplicationCloudWatchLoggingOption -> physical id / Ref = the logging-option Id
//!
//! The sub-resources carry their owning `ApplicationName` in the stack
//! resource's `attributes` (`ApplicationName`) so delete -- which only receives
//! the physical id (the numeric sub-resource id) -- can still locate the app to
//! detach the sub-resource from.

use std::collections::BTreeMap;

use serde_json::Value;

use fakecloud_kinesisanalyticsv2::builders;

use super::{ProvisionResult, ResourceDefinition, StackResource};

impl super::ResourceProvisioner {
    // ------------------------------------------------------------- Application

    pub(super) fn create_ka2_application(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = ka2_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::Application requires ApplicationName".to_string()
        })?;
        let runtime = ka2_str(props, "RuntimeEnvironment").ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::Application requires RuntimeEnvironment".to_string()
        })?;
        let role = ka2_str(props, "ServiceExecutionRole").ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::Application requires ServiceExecutionRole".to_string()
        })?;
        let description = ka2_str(props, "ApplicationDescription");
        let mode = ka2_str(props, "ApplicationMode");

        let mut guard = self.ka2_state.write();
        let st = guard.get_or_create(&self.account_id);
        builders::insert_application(
            st,
            &self.region,
            &self.account_id,
            &name,
            description,
            &runtime,
            &role,
            mode,
            props.get("ApplicationConfiguration"),
            props.get("CloudWatchLoggingOptions"),
            cfn_tags(props),
        )?;
        // Ref / physical id = ApplicationName (Application exposes no GetAtt).
        Ok(ProvisionResult::new(name))
    }

    pub(super) fn update_ka2_application(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Every persisted difference is captured by rebuilding the record from
        // the new desired state under the (possibly new) ApplicationName. When
        // the name is unchanged the ARN / physical id is stable (an in-place
        // update); when it changes, dropping the old record + inserting the new
        // one yields a new physical id, which CloudFormation treats as a
        // replacement. ApplicationName is create-only on the real resource, so
        // this matches its replacement semantics.
        self.delete_ka2_application(&existing.physical_id);
        self.create_ka2_application(resource)
    }

    pub(super) fn delete_ka2_application(&self, physical_id: &str) {
        let mut guard = self.ka2_state.write();
        let st = guard.get_or_create(&self.account_id);
        if let Some(app) = st.applications.remove(physical_id) {
            st.tags.remove(&app.arn);
        }
    }

    // ------------------------------------------------------- ApplicationOutput

    pub(super) fn create_ka2_application_output(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_name = ka2_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::ApplicationOutput requires ApplicationName".to_string()
        })?;
        let output = props.get("Output").cloned().ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::ApplicationOutput requires Output".to_string()
        })?;
        let mut guard = self.ka2_state.write();
        let st = guard.get_or_create(&self.account_id);
        let id = builders::insert_output(st, &app_name, &output)?;
        // Ref / physical id = the output Id; stash the app name for teardown.
        Ok(ProvisionResult::new(id).with("ApplicationName", app_name))
    }

    pub(super) fn update_ka2_application_output(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        if let Some(app_name) = existing.attributes.get("ApplicationName") {
            let mut guard = self.ka2_state.write();
            let st = guard.get_or_create(&self.account_id);
            builders::remove_output(st, app_name, &existing.physical_id);
        }
        self.create_ka2_application_output(resource)
    }

    pub(super) fn delete_ka2_application_output(&self, resource: &StackResource) {
        if let Some(app_name) = resource.attributes.get("ApplicationName") {
            let mut guard = self.ka2_state.write();
            let st = guard.get_or_create(&self.account_id);
            builders::remove_output(st, app_name, &resource.physical_id);
        }
    }

    // -------------------------------------------- ApplicationReferenceDataSource

    pub(super) fn create_ka2_reference_data_source(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_name = ka2_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::ApplicationReferenceDataSource requires ApplicationName"
                .to_string()
        })?;
        let rds = props.get("ReferenceDataSource").cloned().ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::ApplicationReferenceDataSource requires ReferenceDataSource"
                .to_string()
        })?;
        let mut guard = self.ka2_state.write();
        let st = guard.get_or_create(&self.account_id);
        let id = builders::insert_reference_data_source(st, &app_name, &rds)?;
        Ok(ProvisionResult::new(id).with("ApplicationName", app_name))
    }

    pub(super) fn update_ka2_reference_data_source(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        if let Some(app_name) = existing.attributes.get("ApplicationName") {
            let mut guard = self.ka2_state.write();
            let st = guard.get_or_create(&self.account_id);
            builders::remove_reference_data_source(st, app_name, &existing.physical_id);
        }
        self.create_ka2_reference_data_source(resource)
    }

    pub(super) fn delete_ka2_reference_data_source(&self, resource: &StackResource) {
        if let Some(app_name) = resource.attributes.get("ApplicationName") {
            let mut guard = self.ka2_state.write();
            let st = guard.get_or_create(&self.account_id);
            builders::remove_reference_data_source(st, app_name, &resource.physical_id);
        }
    }

    // -------------------------------------- ApplicationCloudWatchLoggingOption

    pub(super) fn create_ka2_cloudwatch_logging_option(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_name = ka2_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption requires ApplicationName"
                .to_string()
        })?;
        let opt = props
            .get("CloudWatchLoggingOption")
            .cloned()
            .ok_or_else(|| {
                "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption requires \
             CloudWatchLoggingOption"
                    .to_string()
            })?;
        let mut guard = self.ka2_state.write();
        let st = guard.get_or_create(&self.account_id);
        let id = builders::insert_cloudwatch_logging_option(st, &app_name, &opt)?;
        Ok(ProvisionResult::new(id).with("ApplicationName", app_name))
    }

    pub(super) fn update_ka2_cloudwatch_logging_option(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        if let Some(app_name) = existing.attributes.get("ApplicationName") {
            let mut guard = self.ka2_state.write();
            let st = guard.get_or_create(&self.account_id);
            builders::remove_cloudwatch_logging_option(st, app_name, &existing.physical_id);
        }
        self.create_ka2_cloudwatch_logging_option(resource)
    }

    pub(super) fn delete_ka2_cloudwatch_logging_option(&self, resource: &StackResource) {
        if let Some(app_name) = resource.attributes.get("ApplicationName") {
            let mut guard = self.ka2_state.write();
            let st = guard.get_or_create(&self.account_id);
            builders::remove_cloudwatch_logging_option(st, app_name, &resource.physical_id);
        }
    }
}

// ---------------------------------------------------------------- helpers

fn ka2_str(props: &Value, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// CloudFormation `Tags` (`[{Key, Value}]`) -> the `{k: v}` map the shared
/// builder stores. The KinesisAnalyticsV2 CFN `Tags` property is always the
/// list-of-pairs form; an object form is accepted too for robustness.
fn cfn_tags(props: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    match props.get("Tags") {
        Some(Value::Array(a)) => {
            for t in a {
                if let Some(k) = t.get("Key").and_then(Value::as_str) {
                    let v = t.get("Value").and_then(Value::as_str).unwrap_or("");
                    out.insert(k.to_string(), v.to_string());
                }
            }
        }
        Some(Value::Object(m)) => {
            for (k, v) in m {
                if let Some(s) = v.as_str() {
                    out.insert(k.clone(), s.to_string());
                }
            }
        }
        _ => {}
    }
    out
}
