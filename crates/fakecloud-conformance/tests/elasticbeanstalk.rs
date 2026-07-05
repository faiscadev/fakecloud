//! Conformance tests for AWS Elastic Beanstalk (awsQuery protocol).
//!
//! Each `#[test_action]` drives the real AWS SDK against a fresh `TestServer`,
//! covering every operation in the vendored Smithy model. The macro validates
//! each `checksum` against the model at compile time, so model drift breaks the
//! build.

mod helpers;

use aws_sdk_elasticbeanstalk::types::{
    ApplicationResourceLifecycleConfig, ConfigurationOptionSetting, EnvironmentHealthAttribute,
    EnvironmentInfoType, S3Location,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

/// The `TestServer` must outlive every SDK call (dropping it kills the
/// spawned fakecloud process), so each helper returns it and the test binds
/// it to a live local.
async fn server() -> TestServer {
    TestServer::start().await
}

async fn server_with_app(name: &str) -> TestServer {
    let s = TestServer::start().await;
    let c = s.elasticbeanstalk_client().await;
    c.create_application()
        .application_name(name)
        .send()
        .await
        .unwrap();
    s
}

async fn server_with_env(app: &str, env: &str) -> TestServer {
    let s = server_with_app(app).await;
    let c = s.elasticbeanstalk_client().await;
    c.create_environment()
        .application_name(app)
        .environment_name(env)
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    s
}

#[test_action("elasticbeanstalk", "CreateApplication", checksum = "34727670")]
#[tokio::test]
async fn create_application() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .create_application()
        .application_name("my-app")
        .description("demo")
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.application().unwrap().application_name(),
        Some("my-app")
    );
}

#[test_action("elasticbeanstalk", "UpdateApplication", checksum = "7cc81a86")]
#[tokio::test]
async fn update_application() {
    let s = server_with_app("upd-app").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .update_application()
        .application_name("upd-app")
        .description("updated")
        .send()
        .await
        .unwrap();
    assert_eq!(out.application().unwrap().description(), Some("updated"));
}

#[test_action(
    "elasticbeanstalk",
    "UpdateApplicationResourceLifecycle",
    checksum = "e0c2a6d8"
)]
#[tokio::test]
async fn update_application_resource_lifecycle() {
    let s = server_with_app("life-app").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .update_application_resource_lifecycle()
        .application_name("life-app")
        .resource_lifecycle_config(
            ApplicationResourceLifecycleConfig::builder()
                .service_role("arn:aws:iam::123456789012:role/eb-service")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(out.application_name(), Some("life-app"));
}

#[test_action("elasticbeanstalk", "DescribeApplications", checksum = "d0a80786")]
#[tokio::test]
async fn describe_applications() {
    let s = server_with_app("desc-app").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c.describe_applications().send().await.unwrap();
    assert!(out
        .applications()
        .iter()
        .any(|a| a.application_name() == Some("desc-app")));
}

#[test_action("elasticbeanstalk", "DeleteApplication", checksum = "04b77783")]
#[tokio::test]
async fn delete_application() {
    let s = server_with_app("del-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.delete_application()
        .application_name("del-app")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "CreateApplicationVersion", checksum = "f40ed5e6")]
#[tokio::test]
async fn create_application_version() {
    let s = server_with_app("ver-app").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .create_application_version()
        .application_name("ver-app")
        .version_label("v1")
        .source_bundle(
            S3Location::builder()
                .s3_bucket("my-bucket")
                .s3_key("app.zip")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.application_version().unwrap().version_label(),
        Some("v1")
    );
}

#[test_action("elasticbeanstalk", "UpdateApplicationVersion", checksum = "564e6df6")]
#[tokio::test]
async fn update_application_version() {
    let s = server_with_app("uver-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.create_application_version()
        .application_name("uver-app")
        .version_label("v1")
        .send()
        .await
        .unwrap();
    let out = c
        .update_application_version()
        .application_name("uver-app")
        .version_label("v1")
        .description("changed")
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.application_version().unwrap().description(),
        Some("changed")
    );
}

#[test_action(
    "elasticbeanstalk",
    "DescribeApplicationVersions",
    checksum = "cd11440a"
)]
#[tokio::test]
async fn describe_application_versions() {
    let s = server_with_app("dver-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.create_application_version()
        .application_name("dver-app")
        .version_label("v1")
        .send()
        .await
        .unwrap();
    let out = c
        .describe_application_versions()
        .application_name("dver-app")
        .send()
        .await
        .unwrap();
    assert!(out
        .application_versions()
        .iter()
        .any(|v| v.version_label() == Some("v1")));
}

#[test_action("elasticbeanstalk", "DeleteApplicationVersion", checksum = "b7a53d52")]
#[tokio::test]
async fn delete_application_version() {
    let s = server_with_app("dlver-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.create_application_version()
        .application_name("dlver-app")
        .version_label("v1")
        .send()
        .await
        .unwrap();
    c.delete_application_version()
        .application_name("dlver-app")
        .version_label("v1")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "CreateEnvironment", checksum = "e32f4280")]
#[tokio::test]
async fn create_environment() {
    let s = server_with_app("env-app").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .create_environment()
        .application_name("env-app")
        .environment_name("env-app-env")
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    assert_eq!(out.status().unwrap().as_str(), "Launching");
    assert!(out.environment_id().unwrap().starts_with("e-"));
}

#[test_action("elasticbeanstalk", "UpdateEnvironment", checksum = "a1203a1f")]
#[tokio::test]
async fn update_environment() {
    let s = server_with_env("uenv-app", "uenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .update_environment()
        .environment_name("uenv-app-env")
        .description("updated env")
        .send()
        .await
        .unwrap();
    assert_eq!(out.status().unwrap().as_str(), "Updating");
}

#[test_action("elasticbeanstalk", "TerminateEnvironment", checksum = "62e108cc")]
#[tokio::test]
async fn terminate_environment() {
    let s = server_with_env("tenv-app", "tenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .terminate_environment()
        .environment_name("tenv-app-env")
        .send()
        .await
        .unwrap();
    assert_eq!(out.status().unwrap().as_str(), "Terminating");
}

#[test_action("elasticbeanstalk", "DescribeEnvironments", checksum = "fc1e3e7b")]
#[tokio::test]
async fn describe_environments() {
    let s = server_with_env("denv-app", "denv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .describe_environments()
        .application_name("denv-app")
        .send()
        .await
        .unwrap();
    assert!(out
        .environments()
        .iter()
        .any(|e| e.environment_name() == Some("denv-app-env")));
}

#[test_action(
    "elasticbeanstalk",
    "DescribeEnvironmentResources",
    checksum = "0103ac40"
)]
#[tokio::test]
async fn describe_environment_resources() {
    let s = server_with_env("renv-app", "renv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .describe_environment_resources()
        .environment_name("renv-app-env")
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.environment_resources().unwrap().environment_name(),
        Some("renv-app-env")
    );
}

#[test_action("elasticbeanstalk", "DescribeEnvironmentHealth", checksum = "aa82e82f")]
#[tokio::test]
async fn describe_environment_health() {
    let s = server_with_env("henv-app", "henv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .describe_environment_health()
        .environment_name("henv-app-env")
        .attribute_names(EnvironmentHealthAttribute::All)
        .send()
        .await
        .unwrap();
    assert_eq!(out.environment_name(), Some("henv-app-env"));
}

#[test_action("elasticbeanstalk", "DescribeInstancesHealth", checksum = "61da8fe9")]
#[tokio::test]
async fn describe_instances_health() {
    let s = server_with_env("ienv-app", "ienv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .describe_instances_health()
        .environment_name("ienv-app-env")
        .send()
        .await
        .unwrap();
    assert!(out.refreshed_at().is_some());
}

#[test_action("elasticbeanstalk", "AbortEnvironmentUpdate", checksum = "a7ab7f96")]
#[tokio::test]
async fn abort_environment_update() {
    let s = server_with_env("aenv-app", "aenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.abort_environment_update()
        .environment_name("aenv-app-env")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "RebuildEnvironment", checksum = "18b56a14")]
#[tokio::test]
async fn rebuild_environment() {
    let s = server_with_env("benv-app", "benv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.rebuild_environment()
        .environment_name("benv-app-env")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "RestartAppServer", checksum = "cfd56cff")]
#[tokio::test]
async fn restart_app_server() {
    let s = server_with_env("senv-app", "senv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.restart_app_server()
        .environment_name("senv-app-env")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "RequestEnvironmentInfo", checksum = "8e80fb5a")]
#[tokio::test]
async fn request_environment_info() {
    let s = server_with_env("qenv-app", "qenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.request_environment_info()
        .environment_name("qenv-app-env")
        .info_type(EnvironmentInfoType::Tail)
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "RetrieveEnvironmentInfo", checksum = "e947dfa3")]
#[tokio::test]
async fn retrieve_environment_info() {
    let s = server_with_env("yenv-app", "yenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.retrieve_environment_info()
        .environment_name("yenv-app-env")
        .info_type(EnvironmentInfoType::Tail)
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticbeanstalk",
    "AssociateEnvironmentOperationsRole",
    checksum = "46e8ccc3"
)]
#[tokio::test]
async fn associate_environment_operations_role() {
    let s = server_with_env("oenv-app", "oenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.associate_environment_operations_role()
        .environment_name("oenv-app-env")
        .operations_role("arn:aws:iam::123456789012:role/eb-ops")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticbeanstalk",
    "DisassociateEnvironmentOperationsRole",
    checksum = "5a215afd"
)]
#[tokio::test]
async fn disassociate_environment_operations_role() {
    let s = server_with_env("penv-app", "penv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.associate_environment_operations_role()
        .environment_name("penv-app-env")
        .operations_role("arn:aws:iam::123456789012:role/eb-ops")
        .send()
        .await
        .unwrap();
    c.disassociate_environment_operations_role()
        .environment_name("penv-app-env")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "SwapEnvironmentCNAMEs", checksum = "da8fecc9")]
#[tokio::test]
async fn swap_environment_cnames() {
    let s = server_with_env("swap-app", "swap-app-src").await;
    let c = s.elasticbeanstalk_client().await;
    c.create_environment()
        .application_name("swap-app")
        .environment_name("swap-app-dst")
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    c.swap_environment_cnames()
        .source_environment_name("swap-app-src")
        .destination_environment_name("swap-app-dst")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticbeanstalk", "ComposeEnvironments", checksum = "b7236427")]
#[tokio::test]
async fn compose_environments() {
    let s = server_with_app("comp-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.compose_environments()
        .application_name("comp-app")
        .group_name("dev")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticbeanstalk",
    "DeleteEnvironmentConfiguration",
    checksum = "9fa1db60"
)]
#[tokio::test]
async fn delete_environment_configuration() {
    let s = server_with_env("cfgenv-app", "cfgenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.delete_environment_configuration()
        .application_name("cfgenv-app")
        .environment_name("cfgenv-app-env")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticbeanstalk",
    "DescribeEnvironmentManagedActions",
    checksum = "39a4ae99"
)]
#[tokio::test]
async fn describe_environment_managed_actions() {
    let s = server_with_env("menv-app", "menv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.describe_environment_managed_actions()
        .environment_name("menv-app-env")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticbeanstalk",
    "DescribeEnvironmentManagedActionHistory",
    checksum = "59ed12cf"
)]
#[tokio::test]
async fn describe_environment_managed_action_history() {
    let s = server_with_env("hmenv-app", "hmenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    c.describe_environment_managed_action_history()
        .environment_name("hmenv-app-env")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticbeanstalk",
    "ApplyEnvironmentManagedAction",
    checksum = "73e89c52"
)]
#[tokio::test]
async fn apply_environment_managed_action() {
    let s = server_with_env("amenv-app", "amenv-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .apply_environment_managed_action()
        .environment_name("amenv-app-env")
        .action_id("action-123")
        .send()
        .await
        .unwrap();
    assert_eq!(out.action_id(), Some("action-123"));
}

#[test_action(
    "elasticbeanstalk",
    "CreateConfigurationTemplate",
    checksum = "fca70a71"
)]
#[tokio::test]
async fn create_configuration_template() {
    let s = server_with_app("tmpl-app").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .create_configuration_template()
        .application_name("tmpl-app")
        .template_name("tmpl1")
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    assert_eq!(out.template_name(), Some("tmpl1"));
}

#[test_action(
    "elasticbeanstalk",
    "UpdateConfigurationTemplate",
    checksum = "ed737e7b"
)]
#[tokio::test]
async fn update_configuration_template() {
    let s = server_with_app("utmpl-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.create_configuration_template()
        .application_name("utmpl-app")
        .template_name("tmpl1")
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    let out = c
        .update_configuration_template()
        .application_name("utmpl-app")
        .template_name("tmpl1")
        .description("changed")
        .send()
        .await
        .unwrap();
    assert_eq!(out.description(), Some("changed"));
}

#[test_action(
    "elasticbeanstalk",
    "DeleteConfigurationTemplate",
    checksum = "4dc814aa"
)]
#[tokio::test]
async fn delete_configuration_template() {
    let s = server_with_app("dtmpl-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.create_configuration_template()
        .application_name("dtmpl-app")
        .template_name("tmpl1")
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    c.delete_configuration_template()
        .application_name("dtmpl-app")
        .template_name("tmpl1")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "elasticbeanstalk",
    "DescribeConfigurationSettings",
    checksum = "461052ed"
)]
#[tokio::test]
async fn describe_configuration_settings() {
    let s = server_with_app("scfg-app").await;
    let c = s.elasticbeanstalk_client().await;
    c.create_configuration_template()
        .application_name("scfg-app")
        .template_name("tmpl1")
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    let out = c
        .describe_configuration_settings()
        .application_name("scfg-app")
        .template_name("tmpl1")
        .send()
        .await
        .unwrap();
    assert!(out
        .configuration_settings()
        .iter()
        .any(|s| s.template_name() == Some("tmpl1")));
}

#[test_action(
    "elasticbeanstalk",
    "DescribeConfigurationOptions",
    checksum = "c9e31322"
)]
#[tokio::test]
async fn describe_configuration_options() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .describe_configuration_options()
        .solution_stack_name("64bit Amazon Linux 2023 v6.1.2 running Node.js 20")
        .send()
        .await
        .unwrap();
    assert!(!out.options().is_empty());
}

#[test_action(
    "elasticbeanstalk",
    "ValidateConfigurationSettings",
    checksum = "39b2d49d"
)]
#[tokio::test]
async fn validate_configuration_settings() {
    let s = server_with_app("valcfg-app").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .validate_configuration_settings()
        .application_name("valcfg-app")
        .option_settings(
            ConfigurationOptionSetting::builder()
                .namespace("aws:autoscaling:asg")
                .option_name("MinSize")
                .value("2")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(out.messages().is_empty());
}

#[test_action("elasticbeanstalk", "DescribeEvents", checksum = "0b01f452")]
#[tokio::test]
async fn describe_events() {
    let s = server_with_env("evt-app", "evt-app-env").await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .describe_events()
        .application_name("evt-app")
        .send()
        .await
        .unwrap();
    assert!(out.events().iter().any(|e| e
        .message()
        .unwrap_or_default()
        .contains("createEnvironment")));
}

#[test_action("elasticbeanstalk", "CheckDNSAvailability", checksum = "e3fa930d")]
#[tokio::test]
async fn check_dns_availability() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .check_dns_availability()
        .cname_prefix("my-unique-cname")
        .send()
        .await
        .unwrap();
    assert_eq!(out.available(), Some(true));
}

#[test_action(
    "elasticbeanstalk",
    "ListAvailableSolutionStacks",
    checksum = "23aa8166"
)]
#[tokio::test]
async fn list_available_solution_stacks() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c.list_available_solution_stacks().send().await.unwrap();
    assert!(!out.solution_stacks().is_empty());
}

#[test_action("elasticbeanstalk", "ListPlatformVersions", checksum = "33badffd")]
#[tokio::test]
async fn list_platform_versions() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c.list_platform_versions().send().await.unwrap();
    assert!(!out.platform_summary_list().is_empty());
}

#[test_action("elasticbeanstalk", "ListPlatformBranches", checksum = "4c770541")]
#[tokio::test]
async fn list_platform_branches() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c.list_platform_branches().send().await.unwrap();
    assert!(!out.platform_branch_summary_list().is_empty());
}

#[test_action("elasticbeanstalk", "DescribePlatformVersion", checksum = "5608d56f")]
#[tokio::test]
async fn describe_platform_version() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .describe_platform_version()
        .platform_arn("arn:aws:elasticbeanstalk:us-east-1::platform/Node.js/1.0")
        .send()
        .await
        .unwrap();
    assert!(out.platform_description().is_some());
}

#[test_action("elasticbeanstalk", "CreatePlatformVersion", checksum = "a01bb4d2")]
#[tokio::test]
async fn create_platform_version() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .create_platform_version()
        .platform_name("custom-platform")
        .platform_version("1.0.0")
        .platform_definition_bundle(
            S3Location::builder()
                .s3_bucket("my-bucket")
                .s3_key("platform.zip")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(out.platform_summary().is_some());
}

#[test_action("elasticbeanstalk", "DeletePlatformVersion", checksum = "e6dd3460")]
#[tokio::test]
async fn delete_platform_version() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c
        .delete_platform_version()
        .platform_arn("arn:aws:elasticbeanstalk:us-east-1:123456789012:platform/custom/1.0.0")
        .send()
        .await
        .unwrap();
    assert!(out.platform_summary().is_some());
}

#[test_action("elasticbeanstalk", "DescribeAccountAttributes", checksum = "d789fa92")]
#[tokio::test]
async fn describe_account_attributes() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c.describe_account_attributes().send().await.unwrap();
    assert!(out.resource_quotas().is_some());
}

#[test_action("elasticbeanstalk", "CreateStorageLocation", checksum = "0d77bee3")]
#[tokio::test]
async fn create_storage_location() {
    let s = server().await;
    let c = s.elasticbeanstalk_client().await;
    let out = c.create_storage_location().send().await.unwrap();
    assert!(out.s3_bucket().unwrap().starts_with("elasticbeanstalk-"));
}

#[test_action("elasticbeanstalk", "ListTagsForResource", checksum = "782e8503")]
#[tokio::test]
async fn list_tags_for_resource() {
    let s = server_with_app("tag-app").await;
    let c = s.elasticbeanstalk_client().await;
    let arn = "arn:aws:elasticbeanstalk:us-east-1:123456789012:application/tag-app";
    let out = c
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(out.resource_arn(), Some(arn));
}

#[test_action("elasticbeanstalk", "UpdateTagsForResource", checksum = "cc05a2b4")]
#[tokio::test]
async fn update_tags_for_resource() {
    let s = server_with_app("utag-app").await;
    let c = s.elasticbeanstalk_client().await;
    let arn = "arn:aws:elasticbeanstalk:us-east-1:123456789012:application/utag-app";
    c.update_tags_for_resource()
        .resource_arn(arn)
        .tags_to_add(
            aws_sdk_elasticbeanstalk::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let out = c
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(out
        .resource_tags()
        .iter()
        .any(|t| t.key() == Some("env") && t.value() == Some("prod")));
}
