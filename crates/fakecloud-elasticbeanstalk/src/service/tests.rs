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
