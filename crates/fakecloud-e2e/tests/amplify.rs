//! End-to-end tests for AWS Amplify, driven through the real `aws-sdk-amplify`
//! client against a live fakecloud server. Exercises the hosting control plane
//! (create app -> get/list -> create branch -> create domain association ->
//! tag/list-tags -> delete), asserting the app/branch/domain state round-trips
//! and the domain-verification lifecycle settles `CREATING` -> `AVAILABLE` on
//! read.

use aws_sdk_amplify::types::{Platform, SubDomainSetting};
use fakecloud_testkit::TestServer;

async fn amplify_client(server: &TestServer) -> aws_sdk_amplify::Client {
    let conf = aws_sdk_amplify::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_amplify::Client::from_conf(conf)
}

#[tokio::test]
async fn amplify_app_branch_domain_lifecycle() {
    let server = TestServer::start().await;
    let client = amplify_client(&server).await;

    // --- Create app ---
    let app = client
        .create_app()
        .name("e2e-app")
        .description("end-to-end app")
        .repository("https://github.com/example/repo")
        .platform(Platform::WebCompute)
        .environment_variables("STAGE", "prod")
        .send()
        .await
        .expect("create_app")
        .app
        .expect("app present");
    let app_id = app.app_id().to_string();
    assert!(app_id.starts_with('d'));
    assert_eq!(app.name(), "e2e-app");
    assert_eq!(app.platform(), &Platform::WebCompute);
    assert_eq!(app.default_domain(), format!("{app_id}.amplifyapp.com"));
    assert!(app.app_arn().starts_with("arn:aws:amplify:"));
    assert!(app.app_arn().contains(&format!(":apps/{app_id}")));
    let app_arn = app.app_arn().to_string();

    // --- Get app (round-trips the create inputs) ---
    let got = client
        .get_app()
        .app_id(&app_id)
        .send()
        .await
        .expect("get_app")
        .app
        .expect("app present");
    assert_eq!(got.description(), "end-to-end app");
    assert_eq!(
        got.environment_variables().get("STAGE").map(String::as_str),
        Some("prod")
    );

    // --- List apps ---
    let apps = client.list_apps().send().await.expect("list_apps").apps;
    assert!(apps.iter().any(|a| a.app_id() == app_id));

    // --- Update app ---
    client
        .update_app()
        .app_id(&app_id)
        .description("updated description")
        .send()
        .await
        .expect("update_app");
    let got = client
        .get_app()
        .app_id(&app_id)
        .send()
        .await
        .expect("get after update")
        .app
        .expect("app present");
    assert_eq!(got.description(), "updated description");

    // --- Create branch ---
    let branch = client
        .create_branch()
        .app_id(&app_id)
        .branch_name("main")
        .stage(aws_sdk_amplify::types::Stage::Production)
        .send()
        .await
        .expect("create_branch")
        .branch
        .expect("branch present");
    assert_eq!(branch.branch_name(), "main");
    assert_eq!(branch.stage(), &aws_sdk_amplify::types::Stage::Production);

    let branches = client
        .list_branches()
        .app_id(&app_id)
        .send()
        .await
        .expect("list_branches")
        .branches;
    assert_eq!(branches.len(), 1);
    assert_eq!(branches[0].branch_name(), "main");

    // --- Create domain association (settles CREATING -> AVAILABLE on read) ---
    let assoc = client
        .create_domain_association()
        .app_id(&app_id)
        .domain_name("example.com")
        .sub_domain_settings(
            SubDomainSetting::builder()
                .prefix("www")
                .branch_name("main")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create_domain_association")
        .domain_association
        .expect("domain association present");
    assert_eq!(assoc.domain_name(), "example.com");
    assert_eq!(
        assoc.domain_status(),
        &aws_sdk_amplify::types::DomainStatus::Creating
    );

    let assoc = client
        .get_domain_association()
        .app_id(&app_id)
        .domain_name("example.com")
        .send()
        .await
        .expect("get_domain_association")
        .domain_association
        .expect("domain association present");
    assert_eq!(
        assoc.domain_status(),
        &aws_sdk_amplify::types::DomainStatus::Available
    );
    assert_eq!(assoc.sub_domains().len(), 1);
    assert!(assoc.sub_domains()[0].verified());

    // --- Start a job and let the build settle to SUCCEED ---
    let started = client
        .start_job()
        .app_id(&app_id)
        .branch_name("main")
        .job_type(aws_sdk_amplify::types::JobType::Release)
        .send()
        .await
        .expect("start_job")
        .job_summary
        .expect("job summary present");
    let job_id = started.job_id().to_string();
    // Read a few times so the deterministic build walks to completion.
    let mut status = started.status().clone();
    for _ in 0..6 {
        if status == aws_sdk_amplify::types::JobStatus::Succeed {
            break;
        }
        status = client
            .get_job()
            .app_id(&app_id)
            .branch_name("main")
            .job_id(&job_id)
            .send()
            .await
            .expect("get_job")
            .job
            .and_then(|j| j.summary)
            .expect("summary")
            .status()
            .clone();
    }
    assert_eq!(status, aws_sdk_amplify::types::JobStatus::Succeed);

    // --- Tagging (apps are ARN-addressed) ---
    client
        .tag_resource()
        .resource_arn(&app_arn)
        .tags("team", "web")
        .tags("env", "prod")
        .send()
        .await
        .expect("tag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&app_arn)
        .send()
        .await
        .expect("list_tags")
        .tags
        .unwrap_or_default();
    assert_eq!(tags.get("team").map(String::as_str), Some("web"));
    assert_eq!(tags.get("env").map(String::as_str), Some("prod"));

    client
        .untag_resource()
        .resource_arn(&app_arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&app_arn)
        .send()
        .await
        .expect("list_tags after untag")
        .tags
        .unwrap_or_default();
    assert!(tags.contains_key("team"));
    assert!(!tags.contains_key("env"));

    // --- Delete branch, then delete app ---
    client
        .delete_branch()
        .app_id(&app_id)
        .branch_name("main")
        .send()
        .await
        .expect("delete_branch");
    client
        .delete_app()
        .app_id(&app_id)
        .send()
        .await
        .expect("delete_app");
    let err = client
        .get_app()
        .app_id(&app_id)
        .send()
        .await
        .expect_err("get after delete should fail");
    assert!(err.into_service_error().is_not_found_exception());
}

#[tokio::test]
async fn amplify_get_missing_app_is_not_found() {
    let server = TestServer::start().await;
    let client = amplify_client(&server).await;
    let err = client
        .get_app()
        .app_id("d000000000000")
        .send()
        .await
        .expect_err("missing app");
    assert!(err.into_service_error().is_not_found_exception());
}

#[tokio::test]
async fn amplify_create_branch_on_missing_app_is_not_found() {
    let server = TestServer::start().await;
    let client = amplify_client(&server).await;
    let err = client
        .create_branch()
        .app_id("d000000000000")
        .branch_name("main")
        .send()
        .await
        .expect_err("missing app");
    assert!(err.into_service_error().is_not_found_exception());
}
