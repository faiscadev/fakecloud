//! End-to-end tests for Amazon MWAA (Managed Workflows for Apache Airflow),
//! driven through the real `aws-sdk-mwaa` client against a live fakecloud
//! server. Exercises the environment lifecycle (create -> get -> list ->
//! update -> tag -> token -> delete), asserting the control-plane state machine
//! settles `CREATING`/`UPDATING` to `AVAILABLE` on read and `DELETING` to gone.

use aws_sdk_mwaa::types::{
    EnvironmentStatus, LoggingConfigurationInput, LoggingLevel, ModuleLoggingConfigurationInput,
    NetworkConfiguration,
};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use fakecloud_testkit::TestServer;

/// Every MWAA operation carries an `@endpoint(hostPrefix)` (`api.` / `env.` /
/// `ops.`) that the SDK prepends to the endpoint host, turning
/// `127.0.0.1:<port>` into the unresolvable `api.127.0.0.1:<port>`. Strip that
/// prefix back off the request URI just before transmit so the request reaches
/// the local test server. (Real AWS resolves `api.airflow.<region>.amazonaws.com`
/// via DNS; a single-host local server does not.)
#[derive(Debug)]
struct StripHostPrefix;

impl Intercept for StripHostPrefix {
    fn name(&self) -> &'static str {
        "StripHostPrefix"
    }

    fn modify_before_transmit(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let req = context.request_mut();
        let uri = req.uri().to_string();
        for prefix in ["//api.", "//env.", "//ops."] {
            if let Some((before, after)) = uri.split_once(prefix) {
                req.set_uri(format!("{before}//{after}"))
                    .expect("rewritten URI is valid");
                break;
            }
        }
        Ok(())
    }
}

async fn mwaa_client(server: &TestServer) -> aws_sdk_mwaa::Client {
    let conf = aws_sdk_mwaa::config::Builder::from(&server.aws_config().await)
        .interceptor(StripHostPrefix)
        .build();
    aws_sdk_mwaa::Client::from_conf(conf)
}

fn network_config() -> NetworkConfiguration {
    NetworkConfiguration::builder()
        .subnet_ids("subnet-0aaaa1111bbbb2222")
        .subnet_ids("subnet-0cccc3333dddd4444")
        .security_group_ids("sg-0eeee5555ffff6666")
        .build()
}

async fn create_env(client: &aws_sdk_mwaa::Client, name: &str) -> String {
    let logging = LoggingConfigurationInput::builder()
        .scheduler_logs(
            ModuleLoggingConfigurationInput::builder()
                .enabled(true)
                .log_level(LoggingLevel::Info)
                .build()
                .unwrap(),
        )
        .build();
    client
        .create_environment()
        .name(name)
        .execution_role_arn("arn:aws:iam::000000000000:role/mwaa-execution")
        .source_bucket_arn("arn:aws:s3:::my-airflow-dags")
        .dag_s3_path("dags")
        .network_configuration(network_config())
        .logging_configuration(logging)
        .max_workers(10)
        .airflow_version("2.10.3")
        .send()
        .await
        .expect("create_environment")
        .arn
        .expect("create returns an Arn")
}

#[tokio::test]
async fn mwaa_environment_lifecycle() {
    let server = TestServer::start().await;
    let client = mwaa_client(&server).await;

    // --- Create ---
    let arn = create_env(&client, "e2e-lifecycle").await;
    assert!(arn.starts_with("arn:aws:airflow:us-east-1:"));
    assert!(arn.ends_with(":environment/e2e-lifecycle"));

    // --- Get (settles CREATING -> AVAILABLE) ---
    let env = client
        .get_environment()
        .name("e2e-lifecycle")
        .send()
        .await
        .expect("get_environment")
        .environment
        .expect("environment present");
    assert_eq!(env.name.as_deref(), Some("e2e-lifecycle"));
    assert_eq!(env.status, Some(EnvironmentStatus::Available));
    assert_eq!(env.arn.as_deref(), Some(arn.as_str()));
    assert_eq!(env.max_workers, Some(10));
    assert_eq!(env.airflow_version.as_deref(), Some("2.10.3"));
    assert!(env
        .webserver_url
        .as_deref()
        .unwrap()
        .ends_with("airflow.amazonaws.com"));
    // The enabled scheduler log module surfaces a CloudWatch log group ARN.
    let sched = env
        .logging_configuration
        .as_ref()
        .and_then(|l| l.scheduler_logs.as_ref())
        .expect("scheduler logs");
    assert_eq!(sched.enabled, Some(true));
    assert!(sched.cloud_watch_log_group_arn.is_some());

    // --- List ---
    let names = client
        .list_environments()
        .send()
        .await
        .expect("list_environments")
        .environments;
    assert!(names.contains(&"e2e-lifecycle".to_string()));

    // --- Update (settles UPDATING -> AVAILABLE with a SUCCESS LastUpdate) ---
    client
        .update_environment()
        .name("e2e-lifecycle")
        .max_workers(25)
        .environment_class("mw1.large")
        .send()
        .await
        .expect("update_environment");
    let env = client
        .get_environment()
        .name("e2e-lifecycle")
        .send()
        .await
        .expect("get after update")
        .environment
        .expect("environment present");
    assert_eq!(env.status, Some(EnvironmentStatus::Available));
    assert_eq!(env.max_workers, Some(25));
    assert_eq!(env.environment_class.as_deref(), Some("mw1.large"));
    assert_eq!(
        env.last_update.as_ref().and_then(|u| u.status.clone()),
        Some(aws_sdk_mwaa::types::UpdateStatus::Success)
    );

    // --- Tagging ---
    client
        .tag_resource()
        .resource_arn(&arn)
        .tags("team", "data-eng")
        .tags("env", "prod")
        .send()
        .await
        .expect("tag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list_tags")
        .tags
        .unwrap_or_default();
    assert_eq!(tags.get("team").map(String::as_str), Some("data-eng"));
    assert_eq!(tags.get("env").map(String::as_str), Some("prod"));

    client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list_tags after untag")
        .tags
        .unwrap_or_default();
    assert!(tags.contains_key("team"));
    assert!(!tags.contains_key("env"));

    // Tags also round-trip on GetEnvironment.
    let env = client
        .get_environment()
        .name("e2e-lifecycle")
        .send()
        .await
        .expect("get for tags")
        .environment
        .expect("environment present");
    assert_eq!(
        env.tags
            .as_ref()
            .and_then(|t| t.get("team"))
            .map(String::as_str),
        Some("data-eng")
    );

    // --- CLI token ---
    let tok = client
        .create_cli_token()
        .name("e2e-lifecycle")
        .send()
        .await
        .expect("create_cli_token");
    assert!(!tok.cli_token.as_deref().unwrap_or_default().is_empty());
    assert!(tok
        .web_server_hostname
        .as_deref()
        .unwrap()
        .ends_with("airflow.amazonaws.com"));

    // --- Delete (settles DELETING -> gone) ---
    client
        .delete_environment()
        .name("e2e-lifecycle")
        .send()
        .await
        .expect("delete_environment");
    let err = client
        .get_environment()
        .name("e2e-lifecycle")
        .send()
        .await
        .expect_err("get after delete should fail");
    let svc_err = err.into_service_error();
    assert!(
        svc_err.is_resource_not_found_exception(),
        "expected ResourceNotFoundException, got {svc_err:?}"
    );
}

#[tokio::test]
async fn mwaa_get_missing_environment_is_not_found() {
    let server = TestServer::start().await;
    let client = mwaa_client(&server).await;
    let err = client
        .get_environment()
        .name("does-not-exist")
        .send()
        .await
        .expect_err("missing environment");
    assert!(err.into_service_error().is_resource_not_found_exception());
}

#[tokio::test]
async fn mwaa_duplicate_name_is_validation_error() {
    let server = TestServer::start().await;
    let client = mwaa_client(&server).await;
    create_env(&client, "dup-env").await;
    let err = client
        .create_environment()
        .name("dup-env")
        .execution_role_arn("arn:aws:iam::000000000000:role/mwaa-execution")
        .source_bucket_arn("arn:aws:s3:::my-airflow-dags")
        .dag_s3_path("dags")
        .network_configuration(network_config())
        .send()
        .await
        .expect_err("duplicate create");
    assert!(err.into_service_error().is_validation_exception());
}
