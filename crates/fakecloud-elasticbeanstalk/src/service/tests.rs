use std::collections::HashMap;
use std::sync::Arc;

use super::*;
use crate::state::EbAccounts;

fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut qp = HashMap::new();
    for (k, v) in params {
        qp.insert((*k).to_string(), (*v).to_string());
    }
    AwsRequest {
        service: "elasticbeanstalk".into(),
        action: action.into(),
        region: "us-east-1".into(),
        account_id: "123456789012".into(),
        request_id: "test-req".into(),
        headers: http::HeaderMap::new(),
        query_params: qp,
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".into(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

fn svc() -> ElasticBeanstalkService {
    ElasticBeanstalkService::new(Arc::new(parking_lot::RwLock::new(EbAccounts::new())))
        .with_settle_ms(0)
}

fn body(svc: &ElasticBeanstalkService, action: &str, params: &[(&str, &str)]) -> String {
    let r = block(svc.handle(req(action, params)));
    String::from_utf8_lossy(r.unwrap().body.expect_bytes()).to_string()
}

fn err_code(svc: &ElasticBeanstalkService, action: &str, params: &[(&str, &str)]) -> String {
    match block(svc.handle(req(action, params))) {
        Err(AwsServiceError::AwsError { code, .. }) => code,
        Err(other) => panic!("unexpected error variant: {other:?}"),
        Ok(_) => panic!("expected error, got Ok"),
    }
}

/// A single long-lived multi-thread runtime shared by all tests, so
/// background settle tasks spawned via `tokio::spawn` survive across
/// successive `block()` calls (a fresh per-call runtime would drop them).
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    })
}

fn block<F>(f: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    runtime().block_on(f)
}

#[test]
fn create_and_describe_application() {
    let s = svc();
    let out = body(&s, "CreateApplication", &[("ApplicationName", "myapp")]);
    assert!(out.contains("<CreateApplicationResponse"));
    assert!(out.contains("<ApplicationName>myapp</ApplicationName>"));
    assert!(out.contains("arn:aws:elasticbeanstalk:us-east-1:123456789012:application/myapp"));

    let desc = body(&s, "DescribeApplications", &[]);
    assert!(desc.contains("<ApplicationName>myapp</ApplicationName>"));
}

#[test]
fn create_application_requires_name() {
    let s = svc();
    assert_eq!(err_code(&s, "CreateApplication", &[]), "MissingParameter");
}

#[test]
fn create_environment_launches_then_settles_ready() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "app1")]);
    let out = body(
        &s,
        "CreateEnvironment",
        &[
            ("ApplicationName", "app1"),
            ("EnvironmentName", "app1-env"),
            (
                "SolutionStackName",
                "64bit Amazon Linux 2023 v6.1.2 running Node.js 20",
            ),
        ],
    );
    assert!(out.contains("<Status>Launching</Status>"));
    assert!(out.contains("<EnvironmentId>e-"));
    assert!(out.contains(".us-east-1.elasticbeanstalk.com"));
    assert!(out.contains("<Health>Grey</Health>"));

    // settle_ms is 0 but the settle runs on a spawned task; give it a tick.
    block(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    });
    let desc = body(&s, "DescribeEnvironments", &[("ApplicationName", "app1")]);
    assert!(desc.contains("<Status>Ready</Status>"), "desc={desc}");
    assert!(desc.contains("<Health>Green</Health>"));
    assert!(desc.contains("<HealthStatus>Ok</HealthStatus>"));
}

#[test]
fn create_environment_emits_event() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "app2")]);
    body(
        &s,
        "CreateEnvironment",
        &[("ApplicationName", "app2"), ("EnvironmentName", "app2-env")],
    );
    let events = body(&s, "DescribeEvents", &[("ApplicationName", "app2")]);
    assert!(events.contains("createEnvironment is starting"));
}

#[test]
fn create_environment_unknown_application_errors() {
    let s = svc();
    assert_eq!(
        err_code(
            &s,
            "CreateEnvironment",
            &[("ApplicationName", "nope"), ("EnvironmentName", "nope-env")],
        ),
        "InvalidParameterValue"
    );
}

#[test]
fn terminate_environment_settles_terminated() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "app3")]);
    body(
        &s,
        "CreateEnvironment",
        &[("ApplicationName", "app3"), ("EnvironmentName", "app3-env")],
    );
    block(async { tokio::time::sleep(std::time::Duration::from_millis(20)).await });
    let out = body(
        &s,
        "TerminateEnvironment",
        &[("EnvironmentName", "app3-env")],
    );
    assert!(out.contains("<Status>Terminating</Status>"));
    block(async { tokio::time::sleep(std::time::Duration::from_millis(20)).await });
    let desc = body(
        &s,
        "DescribeEnvironments",
        &[("ApplicationName", "app3"), ("IncludeDeleted", "true")],
    );
    assert!(desc.contains("<Status>Terminated</Status>"), "desc={desc}");
}

#[test]
fn create_application_version_and_describe() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "app4")]);
    let out = body(
        &s,
        "CreateApplicationVersion",
        &[
            ("ApplicationName", "app4"),
            ("VersionLabel", "v1"),
            ("SourceBundle.S3Bucket", "my-bucket"),
            ("SourceBundle.S3Key", "app.zip"),
        ],
    );
    assert!(out.contains("<VersionLabel>v1</VersionLabel>"));
    assert!(out.contains("<S3Bucket>my-bucket</S3Bucket>"));
    let desc = body(
        &s,
        "DescribeApplicationVersions",
        &[("ApplicationName", "app4")],
    );
    assert!(desc.contains("<VersionLabel>v1</VersionLabel>"));
}

#[test]
fn configuration_template_roundtrip() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "app5")]);
    let out = body(
        &s,
        "CreateConfigurationTemplate",
        &[
            ("ApplicationName", "app5"),
            ("TemplateName", "tmpl1"),
            (
                "SolutionStackName",
                "64bit Amazon Linux 2023 v6.1.2 running Node.js 20",
            ),
            ("OptionSettings.member.1.Namespace", "aws:autoscaling:asg"),
            ("OptionSettings.member.1.OptionName", "MinSize"),
            ("OptionSettings.member.1.Value", "2"),
        ],
    );
    assert!(out.contains("<TemplateName>tmpl1</TemplateName>"));
    assert!(out.contains("<Namespace>aws:autoscaling:asg</Namespace>"));
    let desc = body(
        &s,
        "DescribeConfigurationSettings",
        &[("ApplicationName", "app5"), ("TemplateName", "tmpl1")],
    );
    assert!(desc.contains("<OptionName>MinSize</OptionName>"));
}

#[test]
fn check_dns_availability_reports_available() {
    let s = svc();
    let out = body(&s, "CheckDNSAvailability", &[("CNAMEPrefix", "mycname")]);
    assert!(out.contains("<Available>true</Available>"));
    assert!(out.contains("mycname.us-east-1.elasticbeanstalk.com"));
}

#[test]
fn list_solution_stacks_nonempty() {
    let s = svc();
    let out = body(&s, "ListAvailableSolutionStacks", &[]);
    assert!(out.contains("running Node.js 20"));
    assert!(out.contains("<SolutionStackDetails>"));
}

#[test]
fn account_attributes_has_quotas() {
    let s = svc();
    let out = body(&s, "DescribeAccountAttributes", &[]);
    assert!(out.contains("<ApplicationQuota><Maximum>75</Maximum></ApplicationQuota>"));
}

#[test]
fn tags_add_list_remove() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "app6")]);
    let arn = "arn:aws:elasticbeanstalk:us-east-1:123456789012:application/app6";
    body(
        &s,
        "UpdateTagsForResource",
        &[
            ("ResourceArn", arn),
            ("TagsToAdd.member.1.Key", "env"),
            ("TagsToAdd.member.1.Value", "prod"),
        ],
    );
    let out = body(&s, "ListTagsForResource", &[("ResourceArn", arn)]);
    assert!(out.contains("<Key>env</Key>"));
    assert!(out.contains("<Value>prod</Value>"));
}

#[test]
fn recover_pending_environments_settles_stuck_launching() {
    let state = Arc::new(parking_lot::RwLock::new(EbAccounts::new()));
    {
        let mut g = state.write();
        let acct = g.get_or_create("123456789012");
        acct.applications.insert(
            "recapp".into(),
            crate::state::Application {
                name: "recapp".into(),
                arn: "arn".into(),
                description: None,
                date_created: Utc::now(),
                date_updated: Utc::now(),
                resource_lifecycle_config: Default::default(),
            },
        );
        acct.environments.insert(
            "e-stuck00001".into(),
            crate::state::Environment {
                name: "recenv".into(),
                id: "e-stuck00001".into(),
                arn: "arn".into(),
                application_name: "recapp".into(),
                version_label: None,
                solution_stack_name: None,
                platform_arn: None,
                template_name: None,
                description: None,
                cname: "recenv.x.us-east-1.elasticbeanstalk.com".into(),
                endpoint_url: "ep".into(),
                date_created: Utc::now(),
                date_updated: Utc::now(),
                status: est::LAUNCHING.into(),
                abortable_operation_in_progress: true,
                health: "Grey".into(),
                health_status: "Pending".into(),
                tier_name: "WebServer".into(),
                tier_type: "Standard".into(),
                tier_version: "1.0".into(),
                operations_role: None,
                group_name: None,
                option_settings: vec![],
                generation: 1,
            },
        );
    }
    let s = ElasticBeanstalkService::new(state.clone()).with_settle_ms(0);
    block(async {
        s.recover_pending_environments();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    });
    let g = state.read();
    let env = &g.accounts["123456789012"].environments["e-stuck00001"];
    assert_eq!(env.status, est::READY);
}

// ---------------------------------------------------------------------------
// Review-defect regression tests
// ---------------------------------------------------------------------------

/// A `SnapshotStore` that records the last bytes handed to `save`, so a test
/// can assert what `save_snapshot` actually persists.
#[derive(Default)]
struct CapturingStore {
    last: std::sync::Mutex<Option<Vec<u8>>>,
}

impl SnapshotStore for CapturingStore {
    fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
        Ok(self.last.lock().unwrap().clone())
    }
    fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
        *self.last.lock().unwrap() = Some(bytes.to_vec());
        Ok(())
    }
}

/// Fix #1: tuple-keyed `versions` / `templates` must survive snapshot
/// serialization. Before the tuple-key serde adapter, `serde_json::to_vec`
/// fails with `KeyMustBeAString` and the old save path wrote a 0-byte file.
#[test]
fn persistence_round_trip_preserves_tuple_keyed_maps() {
    let store = Arc::new(CapturingStore::default());
    let s = ElasticBeanstalkService::new(Arc::new(parking_lot::RwLock::new(EbAccounts::new())))
        .with_settle_ms(0)
        .with_snapshot_store(store.clone());
    body(&s, "CreateApplication", &[("ApplicationName", "papp")]);
    body(
        &s,
        "CreateApplicationVersion",
        &[("ApplicationName", "papp"), ("VersionLabel", "pv1")],
    );
    body(
        &s,
        "CreateConfigurationTemplate",
        &[
            ("ApplicationName", "papp"),
            ("TemplateName", "ptmpl"),
            ("OptionSettings.member.1.Namespace", "aws:autoscaling:asg"),
            ("OptionSettings.member.1.OptionName", "MinSize"),
            ("OptionSettings.member.1.Value", "3"),
        ],
    );

    // save_snapshot must have produced non-empty bytes with a version present.
    let bytes = store
        .last
        .lock()
        .unwrap()
        .clone()
        .expect("snapshot should have been saved");
    assert!(!bytes.is_empty(), "snapshot must not be a 0-byte overwrite");

    // Deserialize back and assert the tuple-keyed maps survived intact.
    let snap: ElasticBeanstalkSnapshot =
        serde_json::from_slice(&bytes).expect("snapshot must deserialize");
    assert_eq!(
        snap.schema_version,
        ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION
    );
    let accounts = snap.accounts.expect("accounts present");
    let acct = &accounts.accounts["123456789012"];
    assert!(acct.applications.contains_key("papp"));
    assert!(
        acct.versions
            .contains_key(&("papp".to_string(), "pv1".to_string())),
        "tuple-keyed version must survive round-trip"
    );
    let tmpl = acct
        .templates
        .get(&("papp".to_string(), "ptmpl".to_string()))
        .expect("tuple-keyed template must survive round-trip");
    assert_eq!(tmpl.option_settings.len(), 1);
    assert_eq!(tmpl.option_settings[0].option_name, "MinSize");
}

/// Fix #3: a terminated environment addressed by `EnvironmentId` must not be
/// revived by `UpdateEnvironment`.
#[test]
fn terminated_env_by_id_update_rejected() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "tapp")]);
    let out = body(
        &s,
        "CreateEnvironment",
        &[("ApplicationName", "tapp"), ("EnvironmentName", "tapp-env")],
    );
    let env_id = out
        .split("<EnvironmentId>")
        .nth(1)
        .and_then(|s| s.split("</EnvironmentId>").next())
        .unwrap()
        .to_string();
    block(async { tokio::time::sleep(std::time::Duration::from_millis(20)).await });
    body(
        &s,
        "TerminateEnvironment",
        &[("EnvironmentName", "tapp-env")],
    );
    block(async { tokio::time::sleep(std::time::Duration::from_millis(20)).await });
    // Now terminated. Update by id must be rejected, not resurrect the env.
    let code = err_code(&s, "UpdateEnvironment", &[("EnvironmentId", &env_id)]);
    assert_eq!(code, "InvalidParameterValue");
    let g = s.state.read();
    let env = &g.accounts["123456789012"].environments[&env_id];
    assert_eq!(env.status, est::TERMINATED);
}

/// Fix #4: an in-flight `UpdateEnvironment` superseded by a `TerminateEnvironment`
/// must end Terminated (the terminate's generation token wins), never Ready.
#[test]
fn terminate_supersedes_in_flight_update() {
    let s = ElasticBeanstalkService::new(Arc::new(parking_lot::RwLock::new(EbAccounts::new())))
        .with_settle_ms(80);
    body(&s, "CreateApplication", &[("ApplicationName", "rapp")]);
    let out = body(
        &s,
        "CreateEnvironment",
        &[("ApplicationName", "rapp"), ("EnvironmentName", "rapp-env")],
    );
    let env_id = out
        .split("<EnvironmentId>")
        .nth(1)
        .and_then(|s| s.split("</EnvironmentId>").next())
        .unwrap()
        .to_string();
    block(async { tokio::time::sleep(std::time::Duration::from_millis(120)).await });
    // Env is Ready. Start an update (Updating, settle pending), then terminate
    // before the update settle fires.
    body(
        &s,
        "UpdateEnvironment",
        &[("EnvironmentName", "rapp-env"), ("Description", "x")],
    );
    body(
        &s,
        "TerminateEnvironment",
        &[("EnvironmentName", "rapp-env")],
    );
    // Let both settle tasks run; the stale update settle must no-op.
    block(async { tokio::time::sleep(std::time::Duration::from_millis(200)).await });
    let g = s.state.read();
    let env = &g.accounts["123456789012"].environments[&env_id];
    assert_eq!(
        env.status,
        est::TERMINATED,
        "terminate must win over update"
    );
}

/// Fix #4 (other ordering): terminate first, then a racing update must be
/// rejected and the env still ends Terminated.
#[test]
fn terminate_then_update_rejected() {
    let s = ElasticBeanstalkService::new(Arc::new(parking_lot::RwLock::new(EbAccounts::new())))
        .with_settle_ms(80);
    body(&s, "CreateApplication", &[("ApplicationName", "r2app")]);
    body(
        &s,
        "CreateEnvironment",
        &[
            ("ApplicationName", "r2app"),
            ("EnvironmentName", "r2app-env"),
        ],
    );
    block(async { tokio::time::sleep(std::time::Duration::from_millis(120)).await });
    body(
        &s,
        "TerminateEnvironment",
        &[("EnvironmentName", "r2app-env")],
    );
    // Env is Terminating (80ms settle). Update must be rejected.
    let code = err_code(
        &s,
        "UpdateEnvironment",
        &[("EnvironmentName", "r2app-env"), ("Description", "x")],
    );
    assert_eq!(code, "InvalidParameterValue");
    block(async { tokio::time::sleep(std::time::Duration::from_millis(200)).await });
    let desc = body(
        &s,
        "DescribeEnvironments",
        &[("ApplicationName", "r2app"), ("IncludeDeleted", "true")],
    );
    assert!(desc.contains("<Status>Terminated</Status>"), "desc={desc}");
}

/// Fix #2: DeleteApplication without TerminateEnvByForce on an app with a live
/// environment must error and leave the environment intact.
#[test]
fn delete_application_without_force_errors_and_keeps_env() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "dapp")]);
    body(
        &s,
        "CreateEnvironment",
        &[("ApplicationName", "dapp"), ("EnvironmentName", "dapp-env")],
    );
    block(async { tokio::time::sleep(std::time::Duration::from_millis(20)).await });
    let code = err_code(&s, "DeleteApplication", &[("ApplicationName", "dapp")]);
    assert_eq!(code, "InvalidParameterValue");
    // Application and environment survive.
    let apps = body(&s, "DescribeApplications", &[]);
    assert!(apps.contains("<ApplicationName>dapp</ApplicationName>"));
    let envs = body(&s, "DescribeEnvironments", &[("ApplicationName", "dapp")]);
    assert!(envs.contains("<EnvironmentName>dapp-env</EnvironmentName>"));
}

/// Fix #7: DescribeConfigurationSettings on a missing template errors instead
/// of returning an empty success.
#[test]
fn describe_configuration_settings_missing_template_errors() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "capp")]);
    let code = err_code(
        &s,
        "DescribeConfigurationSettings",
        &[("ApplicationName", "capp"), ("TemplateName", "nope")],
    );
    assert_eq!(code, "InvalidParameterValue");
}

/// Fix #8: CreateApplication rejects a duplicate name.
#[test]
fn create_application_duplicate_errors() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "dupapp")]);
    let code = err_code(&s, "CreateApplication", &[("ApplicationName", "dupapp")]);
    assert_eq!(code, "InvalidParameterValue");
}

/// Fix #6: DescribeEvents Severity is a floor -> TRACE returns INFO events.
#[test]
fn describe_events_severity_floor_returns_info() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "eapp")]);
    body(
        &s,
        "CreateEnvironment",
        &[("ApplicationName", "eapp"), ("EnvironmentName", "eapp-env")],
    );
    // The createEnvironment-is-starting event is INFO.
    let out = body(
        &s,
        "DescribeEvents",
        &[("ApplicationName", "eapp"), ("Severity", "TRACE")],
    );
    assert!(
        out.contains("createEnvironment is starting"),
        "TRACE floor must include INFO events; out={out}"
    );
    // A higher floor excludes the INFO event.
    let out2 = body(
        &s,
        "DescribeEvents",
        &[("ApplicationName", "eapp"), ("Severity", "ERROR")],
    );
    assert!(!out2.contains("createEnvironment is starting"));
}

/// Cap-dropped: duplicate application version label is rejected.
#[test]
fn create_application_version_duplicate_errors() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "vapp")]);
    body(
        &s,
        "CreateApplicationVersion",
        &[("ApplicationName", "vapp"), ("VersionLabel", "v1")],
    );
    let code = err_code(
        &s,
        "CreateApplicationVersion",
        &[("ApplicationName", "vapp"), ("VersionLabel", "v1")],
    );
    assert_eq!(code, "InvalidParameterValue");
}

/// Fix #10: CreateConfigurationTemplate from a SourceConfiguration copies the
/// source template's option settings.
#[test]
fn create_configuration_template_from_source_copies_settings() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "sapp")]);
    body(
        &s,
        "CreateConfigurationTemplate",
        &[
            ("ApplicationName", "sapp"),
            ("TemplateName", "src"),
            ("OptionSettings.member.1.Namespace", "aws:autoscaling:asg"),
            ("OptionSettings.member.1.OptionName", "MaxSize"),
            ("OptionSettings.member.1.Value", "6"),
        ],
    );
    // New template created from the source; no explicit OptionSettings.
    body(
        &s,
        "CreateConfigurationTemplate",
        &[
            ("ApplicationName", "sapp"),
            ("TemplateName", "derived"),
            ("SourceConfiguration.ApplicationName", "sapp"),
            ("SourceConfiguration.TemplateName", "src"),
        ],
    );
    let desc = body(
        &s,
        "DescribeConfigurationSettings",
        &[("ApplicationName", "sapp"), ("TemplateName", "derived")],
    );
    assert!(
        desc.contains("<OptionName>MaxSize</OptionName>"),
        "desc={desc}"
    );
    assert!(desc.contains("<Value>6</Value>"));
}

/// Cap-dropped: a worker-tier environment has no public CNAME / endpoint URL.
#[test]
fn worker_tier_environment_has_no_endpoint() {
    let s = svc();
    body(&s, "CreateApplication", &[("ApplicationName", "wapp")]);
    let out = body(
        &s,
        "CreateEnvironment",
        &[
            ("ApplicationName", "wapp"),
            ("EnvironmentName", "wapp-env"),
            ("Tier.Name", "Worker"),
            ("Tier.Type", "SQS/HTTP"),
        ],
    );
    assert!(
        !out.contains("<EndpointURL>"),
        "worker has no endpoint; out={out}"
    );
    assert!(!out.contains("<CNAME>"), "worker has no CNAME; out={out}");
}

#[test]
fn platform_version_create_describe_list_delete_roundtrip() {
    let svc = svc();
    // Unknown platform describes as a service exception, not a fake "Ready".
    assert_eq!(
        err_code(
            &svc,
            "DescribePlatformVersion",
            &[(
                "PlatformArn",
                "arn:aws:elasticbeanstalk:us-east-1:123456789012:platform/ghost/1.0.0",
            )],
        ),
        "ElasticBeanstalkServiceException"
    );

    // Create persists a custom platform.
    let created = body(
        &svc,
        "CreatePlatformVersion",
        &[
            ("PlatformName", "custom-node"),
            ("PlatformVersion", "1.0.0"),
            ("PlatformDefinitionBundle.S3Bucket", "b"),
            ("PlatformDefinitionBundle.S3Key", "k"),
        ],
    );
    assert!(
        created.contains("<PlatformStatus>Ready</PlatformStatus>"),
        "{created}"
    );
    let arn = "arn:aws:elasticbeanstalk:us-east-1:123456789012:platform/custom-node/1.0.0";

    // Describe returns the created platform.
    let described = body(&svc, "DescribePlatformVersion", &[("PlatformArn", arn)]);
    assert!(
        described.contains("<PlatformName>custom-node</PlatformName>"),
        "{described}"
    );
    assert!(
        described.contains("<PlatformCategory>custom</PlatformCategory>"),
        "{described}"
    );

    // List includes it alongside the managed stacks.
    let listed = body(&svc, "ListPlatformVersions", &[]);
    assert!(listed.contains(arn), "custom platform in list: {listed}");

    // Delete removes it; a subsequent describe is a service exception again.
    body(&svc, "DeletePlatformVersion", &[("PlatformArn", arn)]);
    assert_eq!(
        err_code(&svc, "DescribePlatformVersion", &[("PlatformArn", arn)]),
        "ElasticBeanstalkServiceException"
    );
}
