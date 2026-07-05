//! `AWS::ElasticBeanstalk::*` CloudFormation provisioning for the four modeled
//! types -- Application, ApplicationVersion, Environment and
//! ConfigurationTemplate. Each resource is written through to the
//! `elasticbeanstalk` service state as the same typed record the direct
//! `CreateApplication` / `CreateApplicationVersion` / `CreateEnvironment` /
//! `CreateConfigurationTemplate` handlers store, so a CFN-created resource reads
//! back identically on the matching `Describe*` and persists through the
//! `elasticbeanstalk` snapshot hook (survives a restart -- the #1766
//! phantom-resource lesson).
//!
//! Physical id + `Ref` (verified against the AWS resource specs -- they differ
//! per type):
//!   Application           -> Ref = application name  (physical id = name)
//!   ApplicationVersion    -> Ref = version label     (physical id = label)
//!   Environment           -> Ref = environment name  (physical id = name)
//!   ConfigurationTemplate -> Ref = template name     (physical id = name)
//!
//! `Fn::GetAtt` (verified against the AWS resource specs):
//!   Application           -> (no documented attributes)
//!   ApplicationVersion    -> (no documented attributes)
//!   Environment           -> EndpointURL (the only declared attribute)
//!   ConfigurationTemplate -> (no documented attributes)
//!
//! Environment settle: Elastic Beanstalk's own `CreateEnvironment` returns with
//! `Status=Launching` and settles to `Ready` on a background task. Real
//! CloudFormation does not report the environment resource `CREATE_COMPLETE`
//! until that launch has finished and the environment is healthy, so this
//! synchronous provisioner writes the settled terminal state EB's settle task
//! produces (`Ready` / `Green` / `Ok`, launch events emitted) -- the same state
//! a client would observe once the stack reaches `CREATE_COMPLETE`. This avoids
//! leaving a CFN-provisioned environment stuck in `Launching` (nothing re-drives
//! it mid-session; only a restart runs `recover_pending_environments`).

use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

use fakecloud_elasticbeanstalk::state::{
    environment_status as est, Application, ApplicationVersion, ConfigurationTemplate, Environment,
    MaxAgeRule, MaxCountRule, OptionSetting, ResourceLifecycleConfig, ResourceTag,
    SourceBuildInformation,
};

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

// ARN helpers mirror the private formats the `elasticbeanstalk` service uses so
// a CFN-provisioned resource's ARN is byte-identical to the direct handler's.
fn application_arn(region: &str, account: &str, name: &str) -> String {
    format!("arn:aws:elasticbeanstalk:{region}:{account}:application/{name}")
}

fn application_version_arn(region: &str, account: &str, app: &str, label: &str) -> String {
    format!("arn:aws:elasticbeanstalk:{region}:{account}:applicationversion/{app}/{label}")
}

fn environment_arn(region: &str, account: &str, app: &str, env: &str) -> String {
    format!("arn:aws:elasticbeanstalk:{region}:{account}:environment/{app}/{env}")
}

fn configuration_template_arn(region: &str, account: &str, app: &str, template: &str) -> String {
    format!("arn:aws:elasticbeanstalk:{region}:{account}:configurationtemplate/{app}/{template}")
}

impl ResourceProvisioner {
    // ----------------------------------------------------------- Application

    pub(super) fn create_eb_application(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = eb_str(props, "ApplicationName").unwrap_or_else(|| resource.logical_id.clone());
        let arn = application_arn(&self.region, &self.account_id, &name);
        let now = Utc::now();

        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        // AWS rejects a duplicate application name; it is not idempotent.
        if acct.applications.contains_key(&name) {
            return Err(format!("Application {name} already exists."));
        }
        acct.applications.insert(
            name.clone(),
            Application {
                name: name.clone(),
                arn: arn.clone(),
                description: eb_str(props, "Description"),
                date_created: now,
                date_updated: now,
                resource_lifecycle_config: eb_resource_lifecycle_config(props),
            },
        );
        let tags = eb_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn, tags);
        }

        // Ref = application name.
        Ok(ProvisionResult::new(name))
    }

    pub(super) fn update_eb_application(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let old_name = existing.physical_id.clone();
        let new_name =
            eb_str(props, "ApplicationName").unwrap_or_else(|| existing.logical_id.clone());

        // ApplicationName is create-only on the CFN resource -- a rename
        // re-provisions the application from scratch.
        if new_name != old_name {
            self.delete_eb_application(&old_name);
            return self.create_eb_application(resource);
        }

        let arn = application_arn(&self.region, &self.account_id, &old_name);
        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let app = acct
            .applications
            .get_mut(&old_name)
            .ok_or_else(|| format!("Application {old_name} not yet provisioned"))?;
        // Description and ResourceLifecycleConfig are no-interruption updates.
        app.description = eb_str(props, "Description");
        app.resource_lifecycle_config = eb_resource_lifecycle_config(props);
        app.date_updated = Utc::now();

        let tags = eb_tags(props);
        if tags.is_empty() {
            acct.tags.remove(&arn);
        } else {
            acct.tags.insert(arn, tags);
        }
        Ok(ProvisionResult::new(old_name))
    }

    pub(super) fn delete_eb_application(&self, name: &str) {
        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if !acct.applications.contains_key(name) {
            return;
        }
        // Collect every ARN whose tags must be dropped (app + its versions,
        // templates and environments) so no orphaned tag entry remains --
        // mirrors the direct `DeleteApplication` cascade.
        let mut removed_arns: Vec<String> =
            vec![application_arn(&self.region, &self.account_id, name)];
        removed_arns.extend(
            acct.versions
                .values()
                .filter(|v| v.application_name == name)
                .map(|v| v.arn.clone()),
        );
        removed_arns.extend(acct.templates.keys().filter(|(app, _)| app == name).map(
            |(app, tmpl)| configuration_template_arn(&self.region, &self.account_id, app, tmpl),
        ));
        removed_arns.extend(
            acct.environments
                .values()
                .filter(|e| e.application_name == name)
                .map(|e| e.arn.clone()),
        );
        acct.applications.remove(name);
        acct.versions.retain(|(app, _), _| app != name);
        acct.templates.retain(|(app, _), _| app != name);
        // Force-terminate the application's live environments (as the direct
        // handler does with `TerminateEnvByForce`), settling them synchronously
        // to the terminal Terminated state.
        let now = Utc::now();
        for env in acct
            .environments
            .values_mut()
            .filter(|e| e.application_name == name && e.status != est::TERMINATED)
        {
            env.status = est::TERMINATED.to_string();
            env.health = "Grey".to_string();
            env.health_status = "Suspended".to_string();
            env.abortable_operation_in_progress = false;
            env.generation += 1;
            env.date_updated = now;
        }
        for arn in &removed_arns {
            acct.tags.remove(arn);
        }
    }

    // --------------------------------------------------- ApplicationVersion

    pub(super) fn create_eb_application_version(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_name = eb_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::ElasticBeanstalk::ApplicationVersion requires ApplicationName".to_string()
        })?;
        let label = eb_str(props, "VersionLabel").unwrap_or_else(|| resource.logical_id.clone());
        let arn = application_version_arn(&self.region, &self.account_id, &app_name, &label);
        let now = Utc::now();
        let (bucket, key) = eb_source_bundle(props);

        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if !acct.applications.contains_key(&app_name) {
            return Err(format!("No Application named '{app_name}' found."));
        }
        if acct
            .versions
            .contains_key(&(app_name.clone(), label.clone()))
        {
            return Err(format!("Application Version {label} already exists."));
        }
        acct.versions.insert(
            (app_name.clone(), label.clone()),
            ApplicationVersion {
                application_name: app_name.clone(),
                version_label: label.clone(),
                arn: arn.clone(),
                description: eb_str(props, "Description"),
                source_bundle_bucket: bucket,
                source_bundle_key: key,
                source_build_information: eb_source_build_information(props),
                build_arn: None,
                date_created: now,
                date_updated: now,
                status: "Processed".to_string(),
            },
        );
        let tags = eb_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn, tags);
        }

        // Ref = version label. `ApplicationName` is captured so delete can
        // locate the `(app, label)` state key.
        Ok(ProvisionResult::new(label).with("ApplicationName", app_name))
    }

    pub(super) fn update_eb_application_version(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let old_app = captured_application_name(existing);
        let old_label = existing.physical_id.clone();
        let new_app = eb_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::ElasticBeanstalk::ApplicationVersion requires ApplicationName".to_string()
        })?;
        let new_label =
            eb_str(props, "VersionLabel").unwrap_or_else(|| existing.logical_id.clone());

        // ApplicationName, VersionLabel and SourceBundle are all create-only on
        // the CFN resource -- any change re-provisions the version.
        if new_app != old_app || new_label != old_label {
            self.delete_eb_application_version(existing);
            return self.create_eb_application_version(resource);
        }

        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let version = acct
            .versions
            .get_mut(&(old_app.clone(), old_label.clone()))
            .ok_or_else(|| format!("Application Version {old_label} not yet provisioned"))?;
        // Description is the only no-interruption update.
        version.description = eb_str(props, "Description");
        version.date_updated = Utc::now();
        Ok(ProvisionResult::new(old_label).with("ApplicationName", old_app))
    }

    pub(super) fn delete_eb_application_version(&self, resource: &StackResource) {
        let app = captured_application_name(resource);
        let label = resource.physical_id.clone();
        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if let Some(removed) = acct.versions.remove(&(app, label)) {
            acct.tags.remove(&removed.arn);
        }
    }

    // ----------------------------------------------------------- Environment

    pub(super) fn create_eb_environment(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_name = eb_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::ElasticBeanstalk::Environment requires ApplicationName".to_string()
        })?;
        let env_name =
            eb_str(props, "EnvironmentName").unwrap_or_else(|| resource.logical_id.clone());
        let cname_prefix = eb_str(props, "CNAMEPrefix").unwrap_or_else(|| env_name.clone());
        let (tier_name, tier_type, tier_version) = eb_tier(props);
        let is_worker = tier_name.eq_ignore_ascii_case("Worker");
        let now = Utc::now();
        let env_id = gen_environment_id();
        // Worker-tier environments have no public load balancer, so no CNAME and
        // no endpoint URL (matching the direct `CreateEnvironment` handler).
        let cname = if is_worker {
            String::new()
        } else {
            build_cname(&cname_prefix, &self.region)
        };
        let endpoint_url = if is_worker {
            String::new()
        } else {
            build_endpoint_url(&env_id, &self.region)
        };
        let arn = environment_arn(&self.region, &self.account_id, &app_name, &env_name);

        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if !acct.applications.contains_key(&app_name) {
            return Err(format!("No Application named '{app_name}' found."));
        }
        if acct
            .environments
            .values()
            .any(|e| e.name == env_name && e.status != est::TERMINATED)
        {
            return Err(format!("Environment {env_name} already exists."));
        }
        acct.environments.insert(
            env_id.clone(),
            Environment {
                name: env_name.clone(),
                id: env_id.clone(),
                arn: arn.clone(),
                application_name: app_name.clone(),
                version_label: eb_str(props, "VersionLabel"),
                solution_stack_name: eb_str(props, "SolutionStackName"),
                platform_arn: eb_str(props, "PlatformArn"),
                template_name: eb_str(props, "TemplateName"),
                description: eb_str(props, "Description"),
                cname,
                endpoint_url: endpoint_url.clone(),
                date_created: now,
                date_updated: now,
                // Settled terminal state (see module doc): CFN reports the
                // resource complete only once the environment is up and healthy.
                status: est::READY.to_string(),
                abortable_operation_in_progress: false,
                health: "Green".to_string(),
                health_status: "Ok".to_string(),
                tier_name,
                tier_type,
                tier_version,
                operations_role: eb_str(props, "OperationsRole"),
                group_name: eb_str(props, "GroupName"),
                option_settings: eb_option_settings(props),
                generation: 1,
            },
        );
        let tags = eb_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn, tags);
        }
        // Emit the launch start + completion events the API + settle task
        // produce, so the environment's event stream matches a live launch.
        acct.events.insert(
            0,
            eb_event(
                now,
                format!("createEnvironment is starting for {env_name}."),
                &app_name,
                &env_name,
            ),
        );
        acct.events.insert(
            0,
            eb_event(
                now,
                format!("Successfully launched environment: {env_name}"),
                &app_name,
                &env_name,
            ),
        );

        // Ref = environment name; EndpointURL is the sole declared GetAtt.
        // `ApplicationName` is captured so delete can scope the lookup.
        Ok(ProvisionResult::new(env_name)
            .with("EndpointURL", endpoint_url)
            .with("ApplicationName", app_name))
    }

    pub(super) fn update_eb_environment(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let old_name = existing.physical_id.clone();
        let old_app = captured_application_name(existing);
        let new_name =
            eb_str(props, "EnvironmentName").unwrap_or_else(|| existing.logical_id.clone());

        // EnvironmentName and ApplicationName are create-only -- a change to
        // either re-provisions the environment.
        if new_name != old_name || eb_str(props, "ApplicationName").as_deref() != Some(&old_app) {
            self.delete_eb_environment(existing);
            return self.create_eb_environment(resource);
        }

        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let Some(env) = acct
            .environments
            .values_mut()
            .find(|e| e.name == old_name && e.application_name == old_app)
        else {
            return Err(format!("Environment {old_name} not yet provisioned"));
        };
        // In-place, no-interruption updates.
        if let Some(v) = eb_str(props, "VersionLabel") {
            env.version_label = Some(v);
        }
        if let Some(v) = eb_str(props, "SolutionStackName") {
            env.solution_stack_name = Some(v);
        }
        if let Some(v) = eb_str(props, "PlatformArn") {
            env.platform_arn = Some(v);
        }
        if let Some(v) = eb_str(props, "TemplateName") {
            env.template_name = Some(v);
        }
        if props.get("Description").is_some() {
            env.description = eb_str(props, "Description");
        }
        let new_settings = eb_option_settings(props);
        if !new_settings.is_empty() {
            for setting in new_settings {
                if let Some(existing_setting) = env.option_settings.iter_mut().find(|s| {
                    s.namespace == setting.namespace && s.option_name == setting.option_name
                }) {
                    existing_setting.value = setting.value;
                } else {
                    env.option_settings.push(setting);
                }
            }
        }
        env.date_updated = Utc::now();
        let endpoint_url = env.endpoint_url.clone();
        Ok(ProvisionResult::new(old_name)
            .with("EndpointURL", endpoint_url)
            .with("ApplicationName", old_app))
    }

    pub(super) fn get_att_eb_environment(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        if attribute != "EndpointURL" {
            return None;
        }
        let guard = self.elasticbeanstalk_state.read();
        guard
            .accounts
            .get(&self.account_id)?
            .environments
            .values()
            .find(|e| e.name == physical_id && e.status != est::TERMINATED)
            .map(|e| e.endpoint_url.clone())
    }

    pub(super) fn delete_eb_environment(&self, resource: &StackResource) {
        let name = resource.physical_id.clone();
        let app = captured_application_name(resource);
        let now = Utc::now();
        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        // Terminate the environment (as the direct `TerminateEnvironment`
        // handler does): flip to the terminal Terminated state and drop tags.
        // A default `DescribeEnvironments` no longer returns it.
        if let Some(env) = acct
            .environments
            .values_mut()
            .find(|e| e.name == name && e.application_name == app && e.status != est::TERMINATED)
        {
            env.status = est::TERMINATED.to_string();
            env.health = "Grey".to_string();
            env.health_status = "Suspended".to_string();
            env.abortable_operation_in_progress = false;
            env.generation += 1;
            env.date_updated = now;
            let arn = env.arn.clone();
            acct.tags.remove(&arn);
        }
    }

    // ------------------------------------------------ ConfigurationTemplate

    pub(super) fn create_eb_configuration_template(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let app_name = eb_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::ElasticBeanstalk::ConfigurationTemplate requires ApplicationName".to_string()
        })?;
        let template_name =
            eb_str(props, "TemplateName").unwrap_or_else(|| resource.logical_id.clone());
        let arn =
            configuration_template_arn(&self.region, &self.account_id, &app_name, &template_name);
        let now = Utc::now();

        let mut solution_stack = eb_str(props, "SolutionStackName");
        let mut platform_arn = eb_str(props, "PlatformArn");
        let explicit_settings = eb_option_settings(props);
        let source_app = eb_str_at(props, &["SourceConfiguration", "ApplicationName"]);
        let source_template = eb_str_at(props, &["SourceConfiguration", "TemplateName"]);
        let source_environment_id = eb_str(props, "EnvironmentId");

        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        if !acct.applications.contains_key(&app_name) {
            return Err(format!("No Application named '{app_name}' found."));
        }
        if acct
            .templates
            .contains_key(&(app_name.clone(), template_name.clone()))
        {
            return Err(format!(
                "Configuration Template {template_name} already exists."
            ));
        }
        // Seed option settings + stack from a source template or environment
        // when given (mirrors the direct `CreateConfigurationTemplate`).
        let mut option_settings: Vec<OptionSetting> = Vec::new();
        if let (Some(sa), Some(st)) = (source_app.as_ref(), source_template.as_ref()) {
            let Some(src) = acct.templates.get(&(sa.clone(), st.clone())) else {
                return Err(format!("No Configuration Template named '{st}' found."));
            };
            option_settings = src.option_settings.clone();
            solution_stack = solution_stack.or_else(|| src.solution_stack_name.clone());
            platform_arn = platform_arn.or_else(|| src.platform_arn.clone());
        } else if let Some(eid) = source_environment_id.as_ref() {
            let Some(src) = acct
                .environments
                .get(eid)
                .filter(|e| e.status != est::TERMINATED)
            else {
                return Err(format!("No Environment found for EnvironmentId = '{eid}'."));
            };
            option_settings = src.option_settings.clone();
            solution_stack = solution_stack.or_else(|| src.solution_stack_name.clone());
            platform_arn = platform_arn.or_else(|| src.platform_arn.clone());
        }
        // Explicit OptionSettings override / extend those copied from the source.
        for setting in explicit_settings {
            if let Some(existing) = option_settings
                .iter_mut()
                .find(|s| s.namespace == setting.namespace && s.option_name == setting.option_name)
            {
                existing.value = setting.value;
            } else {
                option_settings.push(setting);
            }
        }

        acct.templates.insert(
            (app_name.clone(), template_name.clone()),
            ConfigurationTemplate {
                application_name: app_name.clone(),
                template_name: template_name.clone(),
                description: eb_str(props, "Description"),
                solution_stack_name: solution_stack,
                platform_arn,
                environment_name: eb_str(props, "EnvironmentName"),
                deployment_status: "deployed".to_string(),
                date_created: now,
                date_updated: now,
                option_settings,
            },
        );
        let tags = eb_tags(props);
        if !tags.is_empty() {
            acct.tags.insert(arn, tags);
        }

        // Ref = template name. `ApplicationName` is captured so delete can
        // locate the `(app, template)` state key.
        Ok(ProvisionResult::new(template_name).with("ApplicationName", app_name))
    }

    pub(super) fn update_eb_configuration_template(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let old_app = captured_application_name(existing);
        let old_template = existing.physical_id.clone();
        let new_app = eb_str(props, "ApplicationName").ok_or_else(|| {
            "AWS::ElasticBeanstalk::ConfigurationTemplate requires ApplicationName".to_string()
        })?;
        let new_template =
            eb_str(props, "TemplateName").unwrap_or_else(|| existing.logical_id.clone());

        // ApplicationName, TemplateName and SolutionStackName are create-only --
        // a change to any re-provisions the template.
        let stack_changed = {
            let guard = self.elasticbeanstalk_state.read();
            let cur = guard
                .accounts
                .get(&self.account_id)
                .and_then(|a| a.templates.get(&(old_app.clone(), old_template.clone())))
                .and_then(|t| t.solution_stack_name.clone());
            let new_stack = eb_str(props, "SolutionStackName");
            new_stack.is_some() && new_stack != cur
        };
        if new_app != old_app || new_template != old_template || stack_changed {
            self.delete_eb_configuration_template(existing);
            return self.create_eb_configuration_template(resource);
        }

        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        let template = acct
            .templates
            .get_mut(&(old_app.clone(), old_template.clone()))
            .ok_or_else(|| format!("Configuration Template {old_template} not yet provisioned"))?;
        template.description = eb_str(props, "Description");
        let new_settings = eb_option_settings(props);
        for setting in new_settings {
            if let Some(existing_setting) = template
                .option_settings
                .iter_mut()
                .find(|s| s.namespace == setting.namespace && s.option_name == setting.option_name)
            {
                existing_setting.value = setting.value;
            } else {
                template.option_settings.push(setting);
            }
        }
        template.date_updated = Utc::now();
        Ok(ProvisionResult::new(old_template).with("ApplicationName", old_app))
    }

    pub(super) fn delete_eb_configuration_template(&self, resource: &StackResource) {
        let app = captured_application_name(resource);
        let template = resource.physical_id.clone();
        let arn = configuration_template_arn(&self.region, &self.account_id, &app, &template);
        let mut guard = self.elasticbeanstalk_state.write();
        let acct = guard.get_or_create(&self.account_id);
        acct.templates.remove(&(app, template));
        acct.tags.remove(&arn);
    }
}

// ---------------------------------------------------------------------------
// Environment id / CNAME / endpoint helpers (mirror the service's private forms)
// ---------------------------------------------------------------------------

fn gen_environment_id() -> String {
    let hex = Uuid::new_v4().simple().to_string();
    format!("e-{}", &hex[..10])
}

fn build_cname(prefix: &str, region: &str) -> String {
    let hash = Uuid::new_v4().simple().to_string();
    format!("{prefix}.{}.{region}.elasticbeanstalk.com", &hash[..8])
}

fn build_endpoint_url(env_id: &str, region: &str) -> String {
    let hash = Uuid::new_v4().simple().to_string();
    format!(
        "awseb-{env_id}-AWSEBLoa{}-{}.{region}.elb.amazonaws.com",
        &hash[..8].to_uppercase(),
        &hash[8..16]
    )
}

// ---------------------------------------------------------------------------
// Property parsing helpers
// ---------------------------------------------------------------------------

/// Read a non-empty string property.
fn eb_str(props: &Value, key: &str) -> Option<String> {
    props
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Read a non-empty string at a nested path (`a.b`).
fn eb_str_at(props: &Value, path: &[&str]) -> Option<String> {
    let mut cur = props;
    for seg in path {
        cur = cur.get(seg)?;
    }
    cur.as_str().filter(|s| !s.is_empty()).map(str::to_string)
}

/// The `ApplicationName` captured on a version / environment / template
/// `StackResource` at create time, used to scope its `(app, ...)` state key.
fn captured_application_name(resource: &StackResource) -> String {
    resource
        .attributes
        .get("ApplicationName")
        .cloned()
        .unwrap_or_default()
}

/// Convert CFN `Tags` (`[{Key,Value}]`) into the service's `ResourceTag` list.
fn eb_tags(props: &Value) -> Vec<ResourceTag> {
    let mut out = Vec::new();
    if let Some(arr) = props.get("Tags").and_then(Value::as_array) {
        for t in arr {
            if let Some(key) = t
                .get("Key")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                let value = t
                    .get("Value")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                out.push(ResourceTag {
                    key: key.to_string(),
                    value,
                });
            }
        }
    }
    out
}

/// Convert the CFN `OptionSettings` property (a list of objects in PascalCase)
/// into the service's `OptionSetting` list.
fn eb_option_settings(props: &Value) -> Vec<OptionSetting> {
    let mut out = Vec::new();
    if let Some(arr) = props.get("OptionSettings").and_then(Value::as_array) {
        for o in arr {
            let namespace = o.get("Namespace").and_then(Value::as_str);
            let option_name = o.get("OptionName").and_then(Value::as_str);
            let (Some(ns), Some(name)) = (namespace, option_name) else {
                continue;
            };
            out.push(OptionSetting {
                resource_name: o
                    .get("ResourceName")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                namespace: ns.to_string(),
                option_name: name.to_string(),
                value: o.get("Value").and_then(Value::as_str).map(str::to_string),
            });
        }
    }
    out
}

/// Parse the CFN `Tier` property (`{Name,Type,Version}`), defaulting to the
/// web-server tier the service uses.
fn eb_tier(props: &Value) -> (String, String, String) {
    let tier = props.get("Tier");
    let read = |key: &str, default: &str| {
        tier.and_then(|t| t.get(key))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(default)
            .to_string()
    };
    (
        read("Name", "WebServer"),
        read("Type", "Standard"),
        read("Version", "1.0"),
    )
}

/// Parse the CFN `SourceBundle` property (`{S3Bucket,S3Key}`).
fn eb_source_bundle(props: &Value) -> (Option<String>, Option<String>) {
    let bundle = props.get("SourceBundle");
    let read = |key: &str| {
        bundle
            .and_then(|b| b.get(key))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    (read("S3Bucket"), read("S3Key"))
}

/// Parse the CFN `SourceBundle`-adjacent `SourceBuildInformation` (used by
/// CodeBuild-built versions) when present.
fn eb_source_build_information(props: &Value) -> Option<SourceBuildInformation> {
    let sbi = props.get("SourceBuildInformation")?;
    Some(SourceBuildInformation {
        source_type: sbi.get("SourceType").and_then(Value::as_str)?.to_string(),
        source_repository: sbi
            .get("SourceRepository")
            .and_then(Value::as_str)?
            .to_string(),
        source_location: sbi
            .get("SourceLocation")
            .and_then(Value::as_str)?
            .to_string(),
    })
}

/// Parse the CFN `ResourceLifecycleConfig` property, defaulting to empty.
fn eb_resource_lifecycle_config(props: &Value) -> ResourceLifecycleConfig {
    let Some(cfg) = props.get("ResourceLifecycleConfig") else {
        return ResourceLifecycleConfig::default();
    };
    let vlc = cfg.get("VersionLifecycleConfig");
    let bool_at =
        |obj: Option<&Value>, key: &str| obj.and_then(|o| o.get(key)).and_then(Value::as_bool);
    let i64_at = |obj: Option<&Value>, key: &str| {
        obj.and_then(|o| o.get(key)).and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
    };
    let max_count_rule = vlc
        .and_then(|v| v.get("MaxCountRule"))
        .map(|r| MaxCountRule {
            enabled: bool_at(Some(r), "Enabled").unwrap_or(false),
            max_count: i64_at(Some(r), "MaxCount"),
            delete_source_from_s3: bool_at(Some(r), "DeleteSourceFromS3"),
        });
    let max_age_rule = vlc.and_then(|v| v.get("MaxAgeRule")).map(|r| MaxAgeRule {
        enabled: bool_at(Some(r), "Enabled").unwrap_or(false),
        max_age_in_days: i64_at(Some(r), "MaxAgeInDays"),
        delete_source_from_s3: bool_at(Some(r), "DeleteSourceFromS3"),
    });
    ResourceLifecycleConfig {
        service_role: eb_str(cfg, "ServiceRole"),
        max_count_rule,
        max_age_rule,
    }
}

/// Build an `INFO` environment lifecycle event.
fn eb_event(
    now: chrono::DateTime<Utc>,
    message: String,
    app_name: &str,
    env_name: &str,
) -> fakecloud_elasticbeanstalk::state::Event {
    fakecloud_elasticbeanstalk::state::Event {
        event_date: now,
        message,
        application_name: Some(app_name.to_string()),
        version_label: None,
        template_name: None,
        environment_name: Some(env_name.to_string()),
        platform_arn: None,
        request_id: Some(Uuid::new_v4().to_string()),
        severity: "INFO".to_string(),
    }
}
